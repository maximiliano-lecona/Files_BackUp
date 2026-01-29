import awswrangler as wr
from datetime import datetime, timedelta
import os
import re
import pandas as pd
from io import StringIO
import tempfile
import os
from dotenv import load_dotenv

load_dotenv()


def load_raw_data_from_athena(
    channels,
    store_ids,
    target_date,
    session,
    database=os.getenv('DATA_BASE_NAME'),
    s3_output=os.getenv('ATHENA_LOCATION')
):
  
    # Definir fecha objetivo
    if target_date is None:
        target_date = datetime.now()
    
    # Formatear fecha para el query
    max_date_limit = target_date.strftime('%Y-%m-%d')
    year = str(target_date.year)
    month = str(target_date.month).zfill(2)
    
    # Construir query
    sql_query = f"""
        WITH max_dates AS (
            SELECT 
                "store id",
                MAX(date) as max_date
            FROM {database}
            WHERE "store id" IN ('{"', '".join(store_ids)}')
              AND channel IN ('{"', '".join(channels)}')
              AND year = '{year}'
              AND month = '{month}'
              AND date <= '{max_date_limit}'
            GROUP BY "store id"
        )
        SELECT r.*
        FROM {database} r
        INNER JOIN max_dates m 
            ON r."store id" = m."store id" 
            AND r.date = m.max_date
        WHERE r."store id" IN ('{"', '".join(store_ids)}')
          AND r.channel IN ('{"', '".join(channels)}')
          AND r.year = '{year}'
          AND r.month = '{month}'
          AND r.date <= '{max_date_limit}'
    """
    
    df = wr.athena.read_sql_query(
        sql=sql_query,
        database=database,
        boto3_session=session,
        s3_output=s3_output,
    )
    
    # Eliminar columnas de partición si existen
    df = df.drop(columns=['year', 'month', 'channel'], errors='ignore')
    
    return df

def clean_competitor_data(df):
    """
    Limpia y transforma el DataFrame de competidores
    NOTA: Todos los campos ya vienen como string desde Athena
    """
    def homogenize_date_format(strDate):
        """
        Funcion para establecer la fecha de un mismo formato "AAAA-MM-DD"
        """
        lstDate_Substrs = strDate.split('-')
        if len(lstDate_Substrs[0]) == 2:
            strDate = lstDate_Substrs[2] + '-' + lstDate_Substrs[1] + '-' + lstDate_Substrs[0]
        return strDate
    
    df = df.drop(columns=['year', 'month', 'channel'], errors='ignore')
    
    # 1. Homogenizar fechas
    df['date'] = df['date'].str.replace('/', '-')
    df['date'] = df['date'].apply(homogenize_date_format)
    print(df["canal"].value_counts())

    # 2. Limpiar stock (eliminar .0 al final)
    df['stock'] = df['stock'].str.replace(r'\.0+$', '', regex=True)
    print(df["canal"].value_counts())

    # 3. Validaciones de UPC
    df = df[
        df['upc'].notna() & 
        (df['upc'] != "") & 
        (df['upc'] != "0") & 
        (df['upc'] != "nan")
    ].reset_index(drop=True)
    df["upc"] = df["upc"].str.lstrip('0')
    print(df["canal"].value_counts())

    # 4. Inicializar columnas (si existe upc wm)
    df['upc wm2'] = df['upc wm']
    print(df["canal"].value_counts())
    
    canales_a_conservar = ["9999_farmaciasdelahorro_promos", "9999_benavides_promos"]

    mask_canal_no_protegido = ~df["store id"].isin(canales_a_conservar)

    mask_precio_invalido = (
        df['final price'].isna() |
        (df['final price'] == '') |
        (df['final price'] == '0') |
        (df['final price'] == '0.0') |
        (df['final price'] == 'nan')
    )

    df = df[~(mask_canal_no_protegido & mask_precio_invalido)].reset_index(drop=True)

    print("se limpio el precio vacio ________________")
    print(df["canal"].value_counts())

    
    df["comp"] = ""
    
    # 7. Limpiar precios (remover $, comas, corchetes, espacios)
    price_cols = ['price', 'sale price', 'final price']
    for col in price_cols:
        if col in df.columns:
            df[col] = df[col].str.replace(r'[\$,\[\] ]', '', regex=True)
    print(df["canal"].value_counts())
    """
    df = df[
        df['price'].notna() &
        ~df['price'].astype(str).str.contains('None', na=False) &
        (df['price'] != '') &
        (df['price'] != '0')
    ].reset_index(drop=True)

    print(df["canal"].value_counts())
    """

    # 9. Validaciones de SKU
    df = df[
        df['sku'].notna() &
        (df['sku'] != '') &
        (df['sku'] != 'nan')
    ].reset_index(drop=True)
    print(df["canal"].value_counts())

    # 10. Deduplicar
    df.drop_duplicates(subset=['date', 'store id', 'sku', 'upc'], inplace=True, ignore_index=True)
    print(df["store id"].value_counts())
        
    return df

def get_last_price_from_s3(df, s3_client, bucket_name, prefix, merge_keys=None):
    """
    Obtiene el last_price del archivo más reciente en S3
    SE MANEJA COMO STRING - sin conversiones de tipo

    Args:
        df: DataFrame actual
        s3_client: Cliente boto3 S3
        bucket_name: Nombre del bucket
        prefix: Prefijo donde están los archivos históricos
        merge_keys: Lista de columnas para hacer merge (default: ['store id', 'sku', 'upc'])
    """

    # Llaves por defecto
    if merge_keys is None:
        merge_keys = ['store id', 'sku', 'upc']

    # Obtener lista de archivos
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    all_objects = response.get('Contents', [])

    if not all_objects:
        df['last_price'] = ""
        return df

    regex = r"\d{4}-\d{2}-\d{2}"

    # Extraer fecha del nombre del archivo
    def extract_date_from_key(key):
        match = re.search(regex, key)
        if match:
            return datetime.strptime(match.group(), "%Y-%m-%d")
        return None

    # Crear lista con fechas y claves
    files_with_dates = [(extract_date_from_key(obj['Key']), obj['Key']) for obj in all_objects]
    files_with_dates = [f for f in files_with_dates if f[0] is not None]

    if not files_with_dates:
        df['last_price'] = ""
        return df

    # Encontrar el archivo más reciente
    latest_file = max(files_with_dates, key=lambda x: x[0])
    latest_file_key = latest_file[1]
    latest_date = latest_file[0].strftime('%Y-%m-%d')

    print(f"Usando archivo para last_price: {latest_file_key}")

    # Descargar archivo desde S3
    obj_response = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
    file_content = obj_response['Body'].read().decode('utf-8')

    # Cargar como DataFrame - COMO STRING
    df_last_price = pd.read_csv(
        StringIO(file_content),
        dtype=str,
        low_memory=False
    )

    # Normalizar nombres de columnas
    df_last_price.columns = df_last_price.columns.str.lower()

    # Eliminar columna 'last_price' si existe
    df_last_price = df_last_price.drop(columns=['last_price'], errors='ignore')

    # Deduplicar histórico usando las llaves especificadas
    df_last_price = df_last_price.drop_duplicates(
        subset=merge_keys,
        keep='first'
    )

    # Renombrar 'final price' a 'last_price_full'
    df_last_price.rename(columns={'final price': 'last_price_full'}, inplace=True)

    # Preparar columnas para merge
    merge_columns = merge_keys + ['last_price_full']
    available_columns = [col for col in merge_columns if col in df_last_price.columns]

    # Merge con datos actuales
    df = pd.merge(
        df,
        df_last_price[available_columns],
        on=merge_keys,
        how='left'
    )

    # Deduplicar después del merge
    df = df.drop_duplicates(
        subset=merge_keys,
        keep='first'
    )

    # Rellenar NaN con string vacío
    df['last_price_full'] = df['last_price_full'].fillna("")

    # Lógica de comparación
    df['last_price'] = df.apply(
        lambda row: "" if (row['last_price_full'] == "" or
                            row['last_price_full'] == row['final price'])
                    else row['last_price_full'],
        axis=1
    )

    # Eliminar columna temporal
    df.drop(columns=['last_price_full'], inplace=True)

    return df

def save_to_s3(df, bucket, prefix, filename, s3_client ,column_order):
    """
    Guarda DataFrame en S3
    
    Args:
        df: DataFrame a guardar
        bucket: nombre del bucket S3
        prefix: carpeta/prefijo en S3
        filename: nombre del archivo
        column_order: orden de columnas (opcional)
    """

    if column_order:
        df = df[column_order]
    
    # Construir la key completa
    key = f"{prefix}/{filename}" if prefix else filename

    
    # Escribir a archivo temporal en disco
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='', encoding='utf-8') as tmp_file:
        tmp_filename = tmp_file.name
        df.to_csv(tmp_filename, index=False)
    
    # Subir a S3
    s3_client.upload_file(tmp_filename, bucket, key)
    
    # Limpiar archivo temporal
    os.remove(tmp_filename)

def validate_and_log_data(
    df,
    store_ids_expected,
    target_date,
    s3_client,
    bucket_name,
    log_prefix,
    data_prefix
):
    """
    Valida el DataFrame, elimina datos problemáticos, documenta todo y guarda log en S3.
    
    Args:
        df: DataFrame a validar
        store_ids_expected: Lista de store_ids que se esperan
        target_date: Fecha objetivo (datetime)
        s3_client: Cliente boto3 S3
        bucket_name: Nombre del bucket
        log_prefix: Prefijo para guardar el log (ej: 'derivables/isdin/logs')
        data_prefix: Prefijo donde están los archivos de datos (para comparación histórica)
    
    Returns:
        tuple: (df_cleaned, validation_summary dict)
    """
    from io import StringIO
    import re
    
    log_lines = []
    validation_summary = {
        'nivel': 'SUCCESS',  # SUCCESS, WARNING, ERROR
        'errores_criticos': [],
        'warnings': [],
        'metricas': {}
    }
    
    # Header del log
    log_lines.append("="*80)
    log_lines.append(f"REPORTE DE VALIDACIÓN - {target_date.strftime('%Y-%m-%d %H:%M:%S')}")
    log_lines.append("="*80)
    log_lines.append("")
    
    # Estadísticas iniciales
    initial_shape = df.shape
    log_lines.append(f"📊 DATOS INICIALES:")
    log_lines.append(f"   Total de registros: {initial_shape[0]:,}")
    log_lines.append(f"   Total de columnas: {initial_shape[1]}")
    log_lines.append("")
    
    # ========== VALIDACIÓN 1: STORE IDs ==========
    log_lines.append("-"*80)
    log_lines.append("1️⃣  VALIDACIÓN DE STORE IDs")
    log_lines.append("-"*80)
    
    df_store_ids = set(df['store id'].unique())
    missing_ids = list(set(store_ids_expected) - df_store_ids)
    extra_ids = list(df_store_ids - set(store_ids_expected))
    
    log_lines.append(f"   Store IDs esperados: {len(store_ids_expected)}")
    log_lines.append(f"   Store IDs encontrados: {len(df_store_ids)}")
    
    if missing_ids:
        validation_summary['nivel'] = 'WARNING'
        validation_summary['warnings'].append(f"Faltan {len(missing_ids)} store IDs")
        log_lines.append(f"   ⚠️  FALTAN {len(missing_ids)} STORE IDs:")
        for store_id in missing_ids:
            log_lines.append(f"      - {store_id}")
    else:
        log_lines.append(f"   ✅ Todos los store IDs esperados están presentes")
    
    if extra_ids:
        log_lines.append(f"   ℹ️  Store IDs adicionales no esperados: {len(extra_ids)}")
        for store_id in extra_ids:
            log_lines.append(f"      - {store_id}")
    
    validation_summary['metricas']['store_ids_esperados'] = len(store_ids_expected)
    validation_summary['metricas']['store_ids_encontrados'] = len(df_store_ids)
    validation_summary['metricas']['store_ids_faltantes'] = len(missing_ids)
    log_lines.append("")
    
    # ========== VALIDACIÓN 2: FINAL PRICE NULOS/VACÍOS ==========
    log_lines.append("-"*80)
    log_lines.append("2️⃣  VALIDACIÓN DE FINAL PRICE")
    log_lines.append("-"*80)
    
    nulos_final_price = df[(df["final price"].isnull()) | (df["final price"] == "")]
    count_nulos = len(nulos_final_price)
    
    log_lines.append(f"   Registros con final price nulo/vacío: {count_nulos:,}")
    
    if count_nulos > 0:
        validation_summary['warnings'].append(f"Se eliminaron {count_nulos} registros con final price vacío")
        log_lines.append(f"   ⚠️  Se encontraron y ELIMINARON {count_nulos} registros")
        
        # Mostrar distribución por canal
        if 'canal' in nulos_final_price.columns:
            dist_canal = nulos_final_price['canal'].value_counts()
            log_lines.append(f"   Distribución por canal:")
            for canal, count in dist_canal.items():
                log_lines.append(f"      - {canal}: {count}")
        
        # ELIMINAR registros con final price nulo/vacío
        df = df.dropna(subset=["final price"])
        df = df[df["final price"] != ""]
    else:
        log_lines.append(f"   ✅ No hay registros con final price nulo/vacío")
    
    validation_summary['metricas']['registros_eliminados_final_price'] = count_nulos
    log_lines.append("")
    
    # ========== VALIDACIÓN 3: LAST PRICE ==========
    log_lines.append("-"*80)
    log_lines.append("3️⃣  VALIDACIÓN DE LAST PRICE")
    log_lines.append("-"*80)
    
    if 'last_price' in df.columns:
        # Registros donde se aplicó last_price (diferente de final price y no nulo)
        last_price_aplicado = df[(df["last_price"] != df["final price"]) & (df["last_price"].notna()) & (df["last_price"] != "")]
        count_last_price = len(last_price_aplicado)
        
        log_lines.append(f"   Registros con last_price aplicado: {count_last_price:,}")
        
        if count_last_price > 0:
            log_lines.append(f"   ✅ Se aplicó last_price a {count_last_price} registros")
            
            # Distribución por canal
            if 'canal' in last_price_aplicado.columns:
                dist_last_price = last_price_aplicado['canal'].value_counts()
                log_lines.append(f"   Distribución por canal:")
                for canal, count in dist_last_price.items():
                    log_lines.append(f"      - {canal}: {count}")
        else:
            log_lines.append(f"   ℹ️  No hay cambios de precio (last_price = final price en todos)")
        
        validation_summary['metricas']['registros_con_last_price'] = count_last_price
    else:
        log_lines.append(f"   ⚠️  Columna 'last_price' no encontrada")
        validation_summary['warnings'].append("Columna last_price no encontrada")
    
    log_lines.append("")
    
    # ========== VALIDACIÓN 4: DUPLICADOS ==========
    log_lines.append("-"*80)
    log_lines.append("4️⃣  VALIDACIÓN DE DUPLICADOS")
    log_lines.append("-"*80)
    
    duplicados = df[df.duplicated(subset=['sku', 'upc', "store id", "final price"], keep=False)]
    count_duplicados = len(duplicados)
    
    log_lines.append(f"   Registros duplicados encontrados: {count_duplicados:,}")
    
    if count_duplicados > 0:
        validation_summary['warnings'].append(f"Se eliminaron {count_duplicados} duplicados")
        log_lines.append(f"   ⚠️  Se encontraron y ELIMINARON {count_duplicados} duplicados")
        
        # Mostrar distribución por canal
        if 'canal' in duplicados.columns:
            dist_canal_dup = duplicados['canal'].value_counts()
            log_lines.append(f"   Distribución de duplicados por canal:")
            for canal, count in dist_canal_dup.items():
                log_lines.append(f"      - {canal}: {count}")
        
        # ELIMINAR duplicados
        df = df.drop_duplicates(subset=['sku', 'upc', "store id", "final price"], keep='first')
    else:
        log_lines.append(f"   ✅ No hay registros duplicados")
    
    validation_summary['metricas']['registros_eliminados_duplicados'] = count_duplicados
    log_lines.append("")

    # ========== VALIDACIÓN 5: DISTRIBUCIÓN STORE ID / FECHA ==========
    log_lines.append("-"*80)
    log_lines.append("5️⃣  DISTRIBUCIÓN STORE ID / FECHA")
    log_lines.append("-"*80)
    
    conteo_fechas_store = df[["store id", "date"]].value_counts().sort_index()

    log_lines.append("   Distribución store id / fecha:")
    for (store, fecha), count in conteo_fechas_store.items():
        log_lines.append(f"      - Store {store} | Fecha {fecha}: {count} registros")
    
    log_lines.append("")

    # Igualamos la fecha target a la fecha de todas las filas
    strToday = target_date.strftime('%Y-%m-%d')
    df['date'] = strToday
    
    # ========== VALIDACIÓN 6: FECHAS (DESPUÉS DE IGUALAR) ==========
    log_lines.append("-"*80)
    log_lines.append("6️⃣  VALIDACIÓN DE FECHAS")
    log_lines.append("-"*80)
    
    fecha_hoy = target_date.strftime("%Y-%m-%d")
    conteo_fechas = df['date'].value_counts()
    
    log_lines.append(f"   Fecha objetivo: {fecha_hoy}")
    log_lines.append(f"   Fechas únicas en el archivo: {len(conteo_fechas)}")
    log_lines.append("")
    log_lines.append("   Distribución de fechas:")
    for fecha, count in conteo_fechas.items():
        es_hoy = "✅" if str(fecha) == fecha_hoy else "  "
        log_lines.append(f"      {es_hoy} {fecha}: {count:,} registros")
    
    if fecha_hoy in conteo_fechas.index.astype(str):
        log_lines.append(f"   ✅ El archivo contiene la fecha objetivo")
        validation_summary['metricas']['tiene_fecha_objetivo'] = True
    else:
        validation_summary['warnings'].append("No contiene fecha objetivo")
        log_lines.append(f"   ⚠️  El archivo NO contiene la fecha objetivo")
        validation_summary['metricas']['tiene_fecha_objetivo'] = False
    
    log_lines.append("")
    
    # ========== VALIDACIÓN 7: COMPARACIÓN CON ARCHIVO ANTERIOR ==========
    log_lines.append("-"*80)
    log_lines.append("7️⃣  COMPARACIÓN CON ARCHIVO ANTERIOR")
    log_lines.append("-"*80)
    
    try:
        # Buscar archivo anterior en S3
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=data_prefix)
        all_objects = response.get('Contents', [])
        
        if not all_objects:
            log_lines.append(f"   ℹ️  No se encontró archivo anterior para comparar")
            validation_summary['metricas']['comparacion_anterior'] = 'no_disponible'
        else:
            regex = r"\d{4}-\d{2}-\d{2}"
            
            def extract_date_from_key(key):
                match = re.search(regex, key)
                if match:
                    return datetime.strptime(match.group(), "%Y-%m-%d")
                return None
            
            # Filtrar archivos anteriores a la fecha objetivo
            files_with_dates = [(extract_date_from_key(obj['Key']), obj['Key']) for obj in all_objects]
            files_with_dates = [f for f in files_with_dates if f[0] is not None and f[0] < target_date]
            
            if not files_with_dates:
                log_lines.append(f"   ℹ️  No se encontró archivo anterior para comparar")
                validation_summary['metricas']['comparacion_anterior'] = 'no_disponible'
            else:
                # Archivo más reciente
                latest_file = max(files_with_dates, key=lambda x: x[0])
                latest_file_key = latest_file[1]
                latest_date = latest_file[0].strftime('%Y-%m-%d')
                
                log_lines.append(f"   Archivo anterior encontrado: {latest_date}")
                
                # Descargar y comparar
                obj_response = s3_client.get_object(Bucket=bucket_name, Key=latest_file_key)
                file_content = obj_response['Body'].read().decode('utf-8')
                df_anterior = pd.read_csv(StringIO(file_content), dtype=str, low_memory=False)
                
                # Normalizar columnas
                df_anterior.columns = df_anterior.columns.str.lower()
                
                # Contar productos por store id
                conteo_actual = df.groupby("store id").size()
                conteo_anterior = df_anterior.groupby("store id").size()
                
                comparacion = pd.DataFrame({
                    "anterior": conteo_anterior,
                    "actual": conteo_actual
                }).fillna(0).astype(int)
                
                comparacion["diferencia"] = comparacion["anterior"] - comparacion["actual"]
                comparacion["porcentaje"] = (comparacion["diferencia"] / comparacion["anterior"] * 100).round(1)
                
                # Ordenar por porcentaje descendente
                comparacion = comparacion.sort_values("porcentaje", ascending=False)
                
                log_lines.append("")
                log_lines.append(f"   {'Store ID':<50} {'Anterior':>10} {'Actual':>10} {'Diferencia':>12} {'%':>8}")
                log_lines.append(f"   {'-'*50} {'-'*10} {'-'*10} {'-'*12} {'-'*8}")
                
                for store_id, row in comparacion.iterrows():
                    diff_symbol = ""
                    if row['diferencia'] > 0:
                        diff_symbol = "⚠️ "
                    elif row['diferencia'] < 0:
                        diff_symbol = "📈 "
                    else:
                        diff_symbol = "✅ "
                    
                    log_lines.append(
                        f"   {diff_symbol}{store_id:<48} {row['anterior']:>10} {row['actual']:>10} "
                        f"{row['diferencia']:>12} {row['porcentaje']:>7.1f}%"
                    )
                
                # Alertas si hay disminuciones significativas
                disminuciones_significativas = comparacion[comparacion["porcentaje"] > 10]
                if len(disminuciones_significativas) > 0:
                    validation_summary['warnings'].append(f"{len(disminuciones_significativas)} store IDs con disminución >10%")
                    log_lines.append("")
                    log_lines.append(f"   ⚠️  {len(disminuciones_significativas)} store IDs con disminución mayor al 10%")
                
                validation_summary['metricas']['comparacion_anterior'] = 'completada'
                validation_summary['metricas']['archivo_anterior_fecha'] = latest_date
                
    except Exception as e:
        log_lines.append(f"   ⚠️  Error al comparar con archivo anterior: {str(e)}")
        validation_summary['warnings'].append(f"Error en comparación: {str(e)}")
        validation_summary['metricas']['comparacion_anterior'] = 'error'
    
    log_lines.append("")
    
    # ========== RESUMEN FINAL ==========
    final_shape = df.shape
    registros_eliminados_total = initial_shape[0] - final_shape[0]
    
    log_lines.append("="*80)
    log_lines.append("📋 RESUMEN FINAL")
    log_lines.append("="*80)
    log_lines.append(f"   Registros iniciales: {initial_shape[0]:,}")
    log_lines.append(f"   Registros finales: {final_shape[0]:,}")
    log_lines.append(f"   Registros eliminados: {registros_eliminados_total:,}")
    log_lines.append(f"   Porcentaje de datos limpiados: {(registros_eliminados_total/initial_shape[0]*100):.2f}%")
    log_lines.append("")
    
    # Determinar nivel final
    if len(validation_summary['errores_criticos']) > 0:
        validation_summary['nivel'] = 'ERROR'
        log_lines.append(f"   🔴 NIVEL: ERROR - Se encontraron problemas críticos")
    elif len(validation_summary['warnings']) > 0:
        validation_summary['nivel'] = 'WARNING'
        log_lines.append(f"   🟡 NIVEL: WARNING - Se encontraron advertencias")
    else:
        validation_summary['nivel'] = 'SUCCESS'
        log_lines.append(f"   🟢 NIVEL: SUCCESS - Validación completada sin problemas")
    
    if validation_summary['warnings']:
        log_lines.append("")
        log_lines.append("   Advertencias:")
        for warning in validation_summary['warnings']:
            log_lines.append(f"      ⚠️  {warning}")
    
    validation_summary['metricas']['registros_finales'] = final_shape[0]
    validation_summary['metricas']['registros_eliminados_total'] = registros_eliminados_total
    
    log_lines.append("")
    log_lines.append("="*80)
    log_lines.append(f"FIN DEL REPORTE")
    log_lines.append("="*80)
    
    # Guardar log en S3
    log_content = "\n".join(log_lines)
    log_filename = f"validation_{target_date.strftime('%Y-%m-%d')}.txt"
    log_key = f"{log_prefix}/{log_filename}"
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=log_key,
        Body=log_content.encode('utf-8'),
        ContentType='text/plain'
    )
    
    print(f"✅ Log de validación guardado en: s3://{bucket_name}/{log_key}")
    
    return df, validation_summary