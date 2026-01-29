import functions_db
import price_change_notifier
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime, timedelta
import pandas as pd
import re
from io import StringIO
from statistics import mean, median, mode, StatisticsError
import traceback

load_dotenv() 

# Iniciar cliente para athena
SESSION_ATHENA = boto3.Session(
    region_name=os.getenv('AWS_DEFAULT_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# Iniciar cliente para s3
SESSION_S3 = boto3.client(
    's3',
    region_name=os.getenv('AWS_DEFAULT_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# Configuracion
LST_CHANNELS = ['farmaciasBenavides', 'farmaciasSanPablo', 'farmaciasdelahorro', 'farmaciasGDL', 'walmart', 'soriana', 'farmaciasDesimilares']

LST_STORE_IDS = ["9999_benavides_promos", '9999_benavides', '9999_farmaciassanpablo', '9999_farmaciasdelahorro', '44100_farmaciasgdl','9999_farmaciasdelahorro_promos', '2345_walmart_retail', '9999_farmaciassimilares']

STR_BUCKET_NAME = 'data-bunker-prod-env'
STR_PREFIX_COMPETITORS = 'derivables/yza/competitors_hist/'
STR_PREFIX_PERMANENCIA = 'derivables/yza/permanencia/'
STR_PREFIX_MATCH = 'derivables/yza/match/'
STR_PREFIX_CLIENT = 'derivables/yza/client/'
STR_PREFIX_UPC_HOMOLOGATION = 'derivables/yza/upc_files/'

COMPETITORS_FLOAT_COLUMNS = ['price', 'final price', 'sale price']

LST_ORDER_COLUMN = [
    "Key", "date", "canal", "sku", "upc", "item", "image",
    "price", "sale price", "sales flag", "upc llave", "final price",
    "upc marca prop", "código interno 1", "category", "subcategory", "url sku", "upc_anterior", "store id", "last_price"
]

# Fechas
TARGET_DATE = datetime.today()
strToday = TARGET_DATE.strftime('%Y-%m-%d')
dtLastMonth = TARGET_DATE - timedelta(days=1)

FILE_NAME = f"yza_competitors_local_{strToday}.csv"
FILE_NAME_PERMANENCIA = "permanencia_precios.csv"


def log_message(message, level="INFO"):
    """Funcion para escribir mensajes de log formateados"""
    print(f"[{level}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")


def homogenize_date_format(strDate):
    """Funcion para establecer la fecha de un mismo formato AAAA-MM-DD"""
    lstDate_Substrs = strDate.split('-')
    if len(lstDate_Substrs[0]) == 2:
        strDate = lstDate_Substrs[2] + '-' + lstDate_Substrs[1] + '-' + lstDate_Substrs[0]
    return strDate


def read_csv_safe(s3_client, bucket, key):
    """Lee un CSV de manera segura manejando diferentes formatos y errores"""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(
            StringIO(obj['Body'].read().decode('utf-8')),
            dtype=str
        )
        log_message(f"Archivo leido correctamente: {key} - {len(df)} filas")
        return df
    except Exception as e:
        log_message(f"Error leyendo archivo {key}: {str(e)}", "ERROR")
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(
                StringIO(obj['Body'].read().decode('utf-8')),
                dtype=str,
                on_bad_lines='skip'
            )
            log_message(f"Archivo leido con parametros alternativos: {key} - {len(df)} filas")
            return df
        except:
            log_message(f"No se pudo leer el archivo: {key}", "ERROR")
            return None


def marca_propia(competitors_df, match_df):
    """Realiza el merge para obtener la informacion de match.csv"""
    merged_df = competitors_df.merge(
        match_df,
        left_on=["upc wm", "canal"],
        right_on=["upcwm_competitor", "competitor"],
        how="left"
    )

    merged_df["upc marca prop"] = merged_df["upc_client"].fillna("")
    merged_df["código interno 1"] = merged_df["sku_client"].fillna("")
    merged_df["Key"] = merged_df["canal"] + "_" + merged_df["upc wm"]

    output_df = merged_df[
        [
            "Key", "date_original", "canal", "category", "subcategory", "sku", "upc", "item", "url sku", "image",
            "price", "sale price", "sales flag", "upc wm", "final price", "upc marca prop", "código interno 1", "upc_anterior"
        ]
    ]
    return output_df


def actualizar_upc_y_codigo(output_df, client_df):
    """Actualiza UPC y codigo interno basado en el archivo client"""
    df_actualizado = output_df.copy()

    df_actualizado.loc[df_actualizado['upc marca prop'].isin(['nan', 'None']), 'upc marca prop'] = ''
    df_actualizado.loc[df_actualizado['upc wm'].isin(['nan', 'None']), 'upc wm'] = ''
    client_df.loc[client_df['ean wm'].isin(['nan', 'None']), 'ean wm'] = ''

    mask_vacios = (df_actualizado['upc marca prop'] == '') | (df_actualizado['upc marca prop'].isna())

    for idx in df_actualizado[mask_vacios].index:
        upc_wm = df_actualizado.loc[idx, 'upc wm']

        if upc_wm != '':
            coincidencia = client_df[client_df['ean wm'] == upc_wm]

            if not coincidencia.empty:
                df_actualizado.loc[idx, 'código interno 1'] = coincidencia.iloc[0]['sku']
                df_actualizado.loc[idx, 'upc marca prop'] = coincidencia.iloc[0]['ean wm']
            else:
                df_actualizado.loc[idx, 'código interno 1'] = ''
                df_actualizado.loc[idx, 'upc marca prop'] = ''
        else:
            df_actualizado.loc[idx, 'código interno 1'] = ''
            df_actualizado.loc[idx, 'upc marca prop'] = ''

    return df_actualizado


def create_upc_wm(strUPC, strChannel=''):
    """Crea el UPC WM con formato de 16 digitos"""
    strUPC = str(strUPC).strip()
    if 'Walmart' in strChannel or 'walmart' in strChannel:
        pass
    else:
        if len(strUPC) > 7:
            strUPC = strUPC[:-1]
    while len(strUPC) < 16:
        strUPC = '0' + strUPC
    return strUPC


def procesar_permanencia(s3_client, consolidado_actual, bucket_name, competitors_prefix, client_prefix):
    """Procesa la permanencia de precios"""
    try:
        log_message("INICIO DE PROCESAMIENTO DE PERMANENCIA")
        log_message(f"Consolidado actual: {len(consolidado_actual)} filas")

        column_upc = "upc llave"
        column_codint = "código interno 1"
        column_price = "final price"
        column_canal = "canal"
        canales_excluir = ["Farmacias del Ahorro - Promos", "Benavides - Plan de Lealtad"]

        df_actual = consolidado_actual.copy()

        expected_columns = {
            column_upc: column_upc,
            column_codint: column_codint,
            column_price: column_price,
            column_canal: column_canal
        }

        cols_mapping = {}
        for expected_col, fixed_name in expected_columns.items():
            exact_match = expected_col in df_actual.columns
            if exact_match:
                if expected_col != fixed_name:
                    cols_mapping[expected_col] = fixed_name
            else:
                for col in df_actual.columns:
                    if col.lower() == expected_col.lower():
                        cols_mapping[col] = fixed_name
                        break

        if cols_mapping:
            log_message(f"Normalizando columnas: {cols_mapping}")
            df_actual = df_actual.rename(columns=cols_mapping)

        for col in [column_upc, column_codint, column_price, column_canal]:
            if col not in df_actual.columns:
                log_message(f"Columna faltante despues de normalizacion: {col}", "ERROR")
                log_message(f"Columnas disponibles: {list(df_actual.columns)}")
                return None

        df_actual = df_actual[~df_actual[column_canal].isin(canales_excluir)]
        log_message(f"Consolidado despues de filtro: {len(df_actual)} filas")

        df_actual[column_codint] = df_actual[column_codint].astype(str)
        df_actual[column_upc] = df_actual[column_upc].astype(str)
        df_actual[column_canal] = df_actual[column_canal].astype(str)
        df_actual[column_codint] = df_actual[column_codint].str.replace(r"\.0$", "", regex=True)

        df_actual[column_price] = pd.to_numeric(df_actual[column_price], errors='coerce')

        df_actual["UPC_llave_cod_interno"] = df_actual[column_upc] + "-" + df_actual[column_codint] + "-" + df_actual[column_canal]

        df_actual = df_actual.groupby(["UPC_llave_cod_interno", column_upc], as_index=False).agg({column_price: 'mean'})
        log_message(f"Consolidado despues de agrupar: {len(df_actual)} filas")

        df_actual = df_actual.rename(columns={column_price: "S43"})

        dfs = [df_actual]

        log_message("Buscando archivos historicos...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=competitors_prefix)

        if 'Contents' not in response:
            log_message(f"No se encontraron archivos en {competitors_prefix}", "ERROR")
            return None

        files = response.get("Contents", [])
        log_message(f"Archivos encontrados: {len(files)}")

        file_dates = []
        martes_count = 0
        for obj in files:
            key = obj["Key"]
            filename = key.split("/")[-1]

            match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
            if match:
                file_date = datetime.strptime(match.group(), "%Y-%m-%d")
                if file_date.weekday() == 1:
                    martes_count += 1
                file_dates.append((file_date, key))
            else:
                log_message(f"Archivo ignorado (sin fecha reconocida): {filename}")

        log_message(f"Total de archivos con fecha valida: {len(file_dates)}")
        log_message(f"Archivos de martes encontrados: {martes_count}")

        file_dates = sorted(file_dates, reverse=True)

        files_to_use = []

        martes_files = [(date, key) for date, key in file_dates if date.weekday() == 1]

        if len(martes_files) >= 6:
            log_message("Usando 6 archivos de martes")
            files_to_use = martes_files[:6]
        else:
            log_message(f"Solo hay {len(martes_files)} archivos de martes, usando los ultimos 6 archivos disponibles")
            files_to_use = file_dates[:6]

        if len(files_to_use) < 4:
            log_message(f"No hay suficientes archivos historicos: {len(files_to_use)}", "ERROR")
            return None

        files_to_use = files_to_use[:6]

        log_message(f"Archivos historicos a usar ({len(files_to_use)}):")
        for date, key in files_to_use:
            log_message(f" - {date.date()} ({date.strftime('%A')}) - {key}")

        for i, (file_date, key) in enumerate(files_to_use[:3]):
            try:
                log_message(f"Leyendo archivo historico [{i+1}/3]: {key}")

                df = read_csv_safe(s3_client, bucket_name, key)

                if df is None or len(df) == 0:
                    log_message(f"No se pudo leer o archivo vacio: {key}", "ERROR")
                    continue

                df.columns = [col.lower() for col in df.columns]

                required_columns = [column_upc, column_price, column_codint, column_canal]
                missing_columns = [col for col in required_columns if col not in df.columns]

                if missing_columns:
                    log_message(f"Columnas no encontradas en {key}: {missing_columns}", "ERROR")
                    continue

                df = df[~df[column_canal].isin(canales_excluir)]

                df[column_codint] = df[column_codint].astype(str)
                df[column_upc] = df[column_upc].astype(str)
                df[column_canal] = df[column_canal].astype(str)
                df[column_codint] = df[column_codint].str.replace(r"\.0$", "", regex=True)

                df[column_price] = pd.to_numeric(df[column_price], errors='coerce')

                df["UPC_llave_cod_interno"] = df[column_upc] + "-" + df[column_codint] + "-" + df[column_canal]

                df = df.groupby(["UPC_llave_cod_interno", column_upc], as_index=False).agg({column_price: 'mean'})

                semana = 42 - i
                df = df.rename(columns={column_price: f"S{semana}"})

                dfs.append(df)
                log_message(f"Archivo historico {i+1} procesado: {len(df)} filas")

            except Exception as e:
                log_message(f"Error procesando archivo historico {key}:", "ERROR")
                log_message(traceback.format_exc(), "ERROR")

        if len(dfs) < 2:
            log_message(f"Insuficientes dataframes procesados: {len(dfs)}", "ERROR")
            return None

        log_message("Unificando dataframes...")
        df_final = dfs[0]
        for i, df in enumerate(dfs[1:], 1):
            log_message(f"Uniendo con dataframe historico {i}...")
            df_final = pd.merge(df_final, df, on=["UPC_llave_cod_interno", column_upc], how="outer")
            log_message(f"Tamano despues de unir: {len(df_final)} filas")

        rename_dict = {"S43": "N-1"}
        if "S42" in df_final.columns:
            rename_dict["S42"] = "N-2"
        if "S41" in df_final.columns:
            rename_dict["S41"] = "N-3"
        if "S40" in df_final.columns:
            rename_dict["S40"] = "N-4"

        df_final.rename(columns=rename_dict, inplace=True)

        for col in ["N-1", "N-2", "N-3", "N-4"]:
            if col not in df_final.columns:
                df_final[col] = None

        log_message("Aplicando escenarios...")

        def procesar_fila(row):
            precios = [row.get("N-4", None), row.get("N-3", None), row.get("N-2", None), row.get("N-1", None)]

            try:
                promedio = round(mean([p for p in precios if not pd.isna(p)]), 2)
            except:
                promedio = None

            try:
                mediana = round(median([p for p in precios if not pd.isna(p)]), 2)
            except:
                mediana = None

            try:
                moda = round(mode([p for p in precios if not pd.isna(p)]), 2)
            except StatisticsError:
                moda = "Sin moda"
            except:
                moda = None

            p40, p41, p42, p43 = precios
            precios_validos = [p for p in precios if not pd.isna(p)]

            escenario = "Sin datos suficientes"
            sugerido = "Datos incompletos"

            if len(precios_validos) < 2:
                return pd.Series([promedio, mediana, moda, sugerido, escenario])

            semanas_disponibles = []
            if not pd.isna(p40):
                semanas_disponibles.append(('S40', p40))
            if not pd.isna(p41):
                semanas_disponibles.append(('S41', p41))
            if not pd.isna(p42):
                semanas_disponibles.append(('S42', p42))
            if not pd.isna(p43):
                semanas_disponibles.append(('S43', p43))

            num_semanas = len(semanas_disponibles)

            if num_semanas == 2:
                if semanas_disponibles[0][1] == semanas_disponibles[1][1]:
                    escenario = "Escenario 1"
                    sugerido = semanas_disponibles[1][1]
                else:
                    escenario = "Escenario 2"
                    sugerido = semanas_disponibles[1][1]

            elif num_semanas == 3:
                precio_repetido = None

                if semanas_disponibles[0][1] == semanas_disponibles[1][1]:
                    precio_repetido = semanas_disponibles[0][1]
                elif semanas_disponibles[1][1] == semanas_disponibles[2][1]:
                    precio_repetido = semanas_disponibles[1][1]

                if precio_repetido is not None:
                    escenario = "Escenario 3"
                    sugerido = precio_repetido
                else:
                    escenario = "Escenario 5"
                    sugerido = "No hay recomendacion por cambio de precio cada semana"

            elif num_semanas == 4:
                from collections import Counter
                contador_precios = Counter([precio for _, precio in semanas_disponibles])

                precios_repetidos_2_veces = [precio for precio, count in contador_precios.items() if count == 2]

                if len(precios_repetidos_2_veces) == 1:
                    escenario = "Escenario 4"
                    sugerido = precios_repetidos_2_veces[0]
                elif len(set(precios_validos)) == 4:
                    escenario = "Escenario 5"
                    sugerido = "No hay recomendacion por cambio de precio cada semana"
                else:
                    precio_mas_frecuente = contador_precios.most_common(1)[0][0]
                    if contador_precios[precio_mas_frecuente] >= 2:
                        escenario = "Escenario 4"
                        sugerido = precio_mas_frecuente
                    else:
                        escenario = "Escenario 5"
                        sugerido = "No aplica"

            else:
                escenario = "Escenario 5"
                sugerido = "Datos exceden escenarios definidos"

            return pd.Series([promedio, mediana, moda, sugerido, escenario])

        df_final[["Precio Promedio", "Precio Mediana", "Precio Moda", "Precio recomendado", "Escenario aplicado"]] = df_final.apply(procesar_fila, axis=1)
        log_message("Escenarios aplicados correctamente")

        log_message("Cruzando con datos de cliente...")

        response_client = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=client_prefix)
        client_files = response_client.get("Contents", [])

        if not client_files:
            log_message("No se encontraron archivos en el folder client.", "WARN")
        else:
            latest_client_file = max(client_files, key=lambda x: x["LastModified"])
            latest_client_key = latest_client_file["Key"]
            log_message(f"Usando archivo client mas reciente: {latest_client_key}")

            try:
                df_client = read_csv_safe(s3_client, bucket_name, latest_client_key)

                if df_client is not None:
                    log_message(f"Archivo client cargado: {len(df_client)} filas")

                    df_client.columns = [col.lower() for col in df_client.columns]

                    sku_col = next((col for col in df_client.columns if col.lower() == 'sku'), None)
                    ean_col = next((col for col in df_client.columns if col.lower() == 'ean'), None)
                    desc_col = next((col for col in df_client.columns if 'descripción' in col.lower() and 'sku' in col.lower()), None)

                    if not sku_col:
                        df_client['SKU'] = ""
                        sku_col = 'SKU'

                    if not ean_col:
                        df_client['EAN'] = ""
                        ean_col = 'EAN'

                    if not desc_col:
                        df_client['Descripción SKU'] = ""
                        desc_col = 'Descripción SKU'

                    df_client = df_client.rename(columns={
                        sku_col: 'SKU',
                        ean_col: 'EAN',
                        desc_col: 'Descripción SKU'
                    })

                    df_final["SKU"] = df_final["UPC_llave_cod_interno"].str.split("-").str[1].replace("nan","Producto sin comparacion")

                    df_final["SKU"] = df_final["SKU"].astype(str)
                    df_client["SKU"] = df_client["SKU"].astype(str)

                    log_message(f"Antes de merge - df_final: {len(df_final)}, df_client: {len(df_client)}")

                    df_merged = pd.merge(
                        df_final,
                        df_client[["SKU", "EAN", "Descripción SKU"]],
                        how="left",
                        on="SKU"
                    )
                    df_final = df_merged
                    log_message(f"Despues de merge - df_final: {len(df_final)} filas")

            except Exception as e:
                log_message(f"Error en procesamiento de client: {str(e)}", "ERROR")
                log_message(traceback.format_exc(), "ERROR")

        df_final["Canal"] = df_final["UPC_llave_cod_interno"].str.split("-").str[-1]

        log_message("Resumen final del DataFrame de permanencia:")
        log_message(f"Filas: {len(df_final)}")
        log_message(f"Columnas: {list(df_final.columns)}")

        if "Escenario aplicado" in df_final.columns and "Canal" in df_final.columns:
            log_message("Distribucion de escenarios por canal:")
            for canal in df_final["Canal"].unique():
                df_canal = df_final[df_final["Canal"] == canal]
                escenarios = df_canal["Escenario aplicado"].value_counts().to_dict()
                log_message(f"  {canal}: {escenarios}")

        log_message("Procesamiento de permanencia completado exitosamente!")
        return df_final

    except Exception as e:
        log_message(f"Error general en procesamiento de permanencia: {str(e)}", "ERROR")
        log_message(traceback.format_exc(), "ERROR")
        return None


# ============================================================================
# INICIO DEL PROCESO ETL
# ============================================================================

log_message("==== INICIANDO PROCESO ETL PARA YZA ====")

# 1. Cargar datos desde Athena
log_message("Cargando datos desde Athena...")
df = functions_db.load_raw_data_from_athena(
    LST_CHANNELS,
    LST_STORE_IDS,
    TARGET_DATE,
    SESSION_ATHENA
)
log_message(f"Datos cargados desde Athena: {len(df)} filas")

# 2. Limpiar datos con SKU y final price
df = df.dropna(subset=['sku', 'final price'])
df['upc'] = df['upc'].fillna("unknown")
log_message(f"Despues de limpiar nulos en sku/final price: {len(df)} filas")

# 3. Procesar fechas
df['date'] = df['date'].str.replace('/', '-')
df['date'] = df['date'].apply(homogenize_date_format)
df = df.rename(columns={'date': 'date_original'})

df['date_original'] = pd.to_datetime(df['date_original'])
df = df[df['date_original'] <= pd.to_datetime(strToday)]
log_message(f"Filtrado por fecha limite ({strToday}): {len(df)} filas")

df = df[df.groupby('store id')['date_original'].transform('max') == df['date_original']]
log_message(f"Filtrado por fecha maxima por store: {len(df)} filas")

# 4. Limpiar UPC
df['upc'] = df['upc'].mask(df['upc'].eq("unknown"), df['sku'])
df['upc'] = df['upc'].fillna(df['sku'])
df = df[df['upc'] != "0"].reset_index(drop=True)

# 5. Limpiar precios
for var_price in COMPETITORS_FLOAT_COLUMNS:
    if var_price in df.columns:
        df[var_price] = df[var_price].astype(str).str.replace(r'\$|,|\[|\]| ', '', regex=True)

# 6. Filtrar filas con SKU vacio
df = df[df['sku'].notna()].reset_index(drop=True)
df = df[df['sku'] != ''].reset_index(drop=True)

# 7. Homologacion de UPCs
log_message("Iniciando homologacion de UPCs...")

df['upc'] = df['upc'].str.lstrip('0')
df['upc_anterior'] = df['upc']

response_homologation = SESSION_S3.list_objects_v2(
    Bucket=STR_BUCKET_NAME,
    Prefix=STR_PREFIX_UPC_HOMOLOGATION
)

homologation_keys = [
    obj['Key'] for obj in response_homologation.get('Contents', [])
    if 'yza_upc_homologation.csv' in obj['Key']
]

if homologation_keys:
    obj_homologation = SESSION_S3.get_object(
        Bucket=STR_BUCKET_NAME,
        Key=homologation_keys[0]
    )
    df_upc_homologation = pd.read_csv(
        StringIO(obj_homologation['Body'].read().decode('latin-1')),
        dtype=str
    )

    log_message(f"Archivo de homologacion cargado: {len(df_upc_homologation)} registros")

    df_upc_homologation.columns = df_upc_homologation.columns.str.lower()
    df_upc_homologation['upc_extraccion'] = df_upc_homologation['upc_extraccion'].astype(str).str.strip()
    df_upc_homologation['upc_homologado'] = df_upc_homologation['upc_homologado'].astype(str).str.strip()

    upc_cambios = dict(zip(
        df_upc_homologation['upc_extraccion'],
        df_upc_homologation['upc_homologado']
    ))

    df['upc'] = df['upc'].replace(upc_cambios)

    upcs_cambiados = df['upc'].isin(upc_cambios.values()).sum()
    log_message(f"Homologacion completada: {upcs_cambiados} UPCs actualizados")
else:
    log_message("No se encontro archivo de homologacion de UPCs", "WARN")

# 8. Normalizar nombres de canales
canales = {
    'Farmacias del Ahorro - Online': 'Farmacias del Ahorro',
    'Farmacias GDL - Online': 'Farmacias GDL',
    'Farmacias San Pablo - Online': 'Farmacias San Pablo',
    'Walmart - Tepeyac': 'Walmart',
    'Farmacias Similares': 'Similares CDMX',
    'Benavides - Online': 'Benavides',
    'Soriana - MIYANA': 'Soriana'
}
df['canal'] = df['canal'].replace(canales)

# 9. Eliminar duplicados
df.drop_duplicates(inplace=True, subset=['date_original', 'canal', 'sku', 'upc'])
df.reset_index(drop=True, inplace=True)

# 10. Separar por canal y eliminar duplicados
df_farmacias = df[df["canal"] == "Farmacias GDL"]
df_otros = df[df["canal"] != "Farmacias GDL"]

df_otros = df_otros.drop_duplicates(subset=["sku", "date_original", "store id"], keep="first")

df = pd.concat([df_farmacias, df_otros])
log_message(f"Despues de procesar por canal: {len(df)} filas")

# 11. Cargar archivos match y client desde S3
log_message("Cargando archivos match.csv y client.csv...")

response_match = SESSION_S3.list_objects_v2(Bucket=STR_BUCKET_NAME, Prefix=STR_PREFIX_MATCH)
match_keys = [obj['Key'] for obj in response_match.get('Contents', []) if 'match.csv' in obj['Key']]

if not match_keys:
    log_message("Faltan archivos match.csv", "ERROR")
    raise Exception("Archivo match.csv no encontrado")

response_client = SESSION_S3.list_objects_v2(Bucket=STR_BUCKET_NAME, Prefix=STR_PREFIX_CLIENT)
client_keys = [obj['Key'] for obj in response_client.get('Contents', []) if 'client.csv' in obj['Key']]

if not client_keys:
    log_message("Faltan archivos client.csv", "ERROR")
    raise Exception("Archivo client.csv no encontrado")

obj_match = SESSION_S3.get_object(Bucket=STR_BUCKET_NAME, Key=match_keys[0])
match_df = pd.read_csv(StringIO(obj_match['Body'].read().decode('utf-8')), dtype=str)
match_df.columns = match_df.columns.str.lower()
log_message(f"Archivo match cargado: {len(match_df)} filas")

obj_client = SESSION_S3.get_object(Bucket=STR_BUCKET_NAME, Key=client_keys[0])
client_df = pd.read_csv(StringIO(obj_client['Body'].read().decode('utf-8')), dtype=str)
client_df.columns = client_df.columns.str.lower()
log_message(f"Archivo client cargado: {len(client_df)} filas")

# 12. Procesamiento especial para Farmacias Ahorro Promos
mask = df['canal'] == 'Farmacias del Ahorro - Promos'
if mask.sum() > 0:
    df.loc[mask, 'subcategory'] = df.loc[mask, 'item characteristics']
    df.loc[mask, 'sales flag'] = df.loc[mask, 'store address']
    df.loc[mask, 'upc wm'] = df.loc[mask, 'stock']
    log_message("Procesamiento especial para Farmacias del Ahorro - Promos realizado")

# 13. Procesamiento especial para Benavides Plan de Lealtad
mask_benavides = df['canal'] == 'Benavides - Plan de Lealtad'
if mask_benavides.sum() > 0:
    df.loc[mask_benavides, 'subcategory'] = df.loc[mask_benavides, 'item characteristics']
    df.loc[mask_benavides, 'sales flag'] = df.loc[mask_benavides, 'store address']
    df.loc[mask_benavides, 'upc wm'] = df.loc[mask_benavides, 'stock']
    log_message("Procesamiento especial para Benavides - Plan de Lealtad realizado")

# 14. Limpiar columnas antes del merge
df["upc wm"] = df["upc wm"].astype(str).str.strip()
df["canal"] = df["canal"].astype(str).str.strip()
match_df["upcwm_competitor"] = match_df["upcwm_competitor"].astype(str).str.strip()
match_df["competitor"] = match_df["competitor"].astype(str).str.strip()
client_df["ean wm"] = client_df["ean wm"].astype(str).str.strip()
client_df["ean"] = client_df["ean"].astype(str).str.strip()

# 15. Aplicar funciones marca_propia y actualizar_upc_y_codigo
log_message("Aplicando funciones marca_propia y actualizar_upc_y_codigo...")
output_df = marca_propia(df, match_df)
output_df = actualizar_upc_y_codigo(output_df, client_df)
log_message(f"DataFrame de salida: {len(output_df)} filas")

# 16. Renombrar columnas
output_df = output_df.rename(columns={'upc wm': 'upc llave', 'date_original': 'date'})
output_df['date'] = strToday

# 17. Aplicar recalculo de UPC llave
output_df['upc llave'] = output_df.apply(
    lambda row: row['upc llave'] if row['canal'] in ['Farmacias del Ahorro - Promos', 'Benavides - Plan de Lealtad']
    else create_upc_wm(row['upc'], row['canal']),
    axis=1
)

# 17.5 LOGICA MOUNJARO
log_message("Aplicando logica Mounjaro...")
upcs_mounjaro = [
    "7501082243741",
    "7501082243727",
    "7501082243710",
    "7501082243734",
    "7501082243642",
    "7501082243635"
]

mask_upc = output_df["upc"].astype(str).isin(upcs_mounjaro)

output_df["price_numeric"] = pd.to_numeric(output_df["price"], errors='coerce')

map_desc_benavides = {
    "5.13% de desc": 94.90180,
    "20.2% de desc": 79.87270,
    "40.3% de desc": 59.74536,
    "20.48% de desc": 79.60690,
    "19.25% de desc": 80.78000,
    "38.44% de desc": 61.55915,
}

mask_benavides = (
    mask_upc &
    (output_df["canal"] == "Benavides") &
    output_df["sales flag"].isin(map_desc_benavides)
)

output_df.loc[mask_benavides, "descuento"] = output_df.loc[mask_benavides, "sales flag"].map(map_desc_benavides)

map_desc_ahorro = {
    "https://www.fahorro.com/media/cataloglabel/7501082243741.png": 94.90180,
    "https://www.fahorro.com/media/cataloglabel/7501082243727.png": 79.87270,
    "https://www.fahorro.com/media/cataloglabel/7501082243710.png": 59.74536,
    "https://www.fahorro.com/media/cataloglabel/7501082243734.png": 79.60690,
    "https://www.fahorro.com/media/cataloglabel/7501082243642.png": 80.78000,
    "https://www.fahorro.com/media/cataloglabel/7501082243635.png": 61.55915,
}

mask_ahorro = (
    mask_upc &
    (output_df["canal"] == "Farmacias del Ahorro") &
    output_df["sales flag"].isin(map_desc_ahorro)
)

output_df.loc[mask_ahorro, "descuento"] = output_df.loc[mask_ahorro, "sales flag"].map(map_desc_ahorro)

output_df["descuento"] = pd.to_numeric(output_df["descuento"], errors='coerce').fillna(0)

mask_apply = mask_benavides | mask_ahorro

output_df.loc[mask_apply, "sale price"] = (
    output_df.loc[mask_apply, "price_numeric"] * (output_df.loc[mask_apply, "descuento"] / 100)
).round(2)

output_df.loc[mask_apply, "final price"] = output_df.loc[mask_apply, "sale price"]

output_df.loc[mask_apply, "sale price"] = output_df.loc[mask_apply, "sale price"].astype(str)
output_df.loc[mask_apply, "final price"] = output_df.loc[mask_apply, "final price"].astype(str)

output_df.drop(columns=["price_numeric", "descuento"], inplace=True)

log_message(f"Logica Mounjaro aplicada: {mask_apply.sum()} registros afectados")
# FIN LOGICA MOUNJARO

# 18. Last price - usando funcion de functions_db con merge_keys personalizadas
log_message("Procesando last_price...")
output_df = functions_db.get_last_price_from_s3(
    output_df,
    SESSION_S3,
    STR_BUCKET_NAME,
    STR_PREFIX_COMPETITORS,
    merge_keys=['canal', 'sku', 'upc']  # YZA usa canal en lugar de store id
)
log_message("Last price procesado correctamente")

# 19. Proceso de permanencia
log_message("==== INICIANDO PROCESO DE PERMANENCIA DE PRECIOS ====")

consolidado_actual = output_df.copy()

df_permanencia = procesar_permanencia(
    s3_client=SESSION_S3,
    consolidado_actual=consolidado_actual,
    bucket_name=STR_BUCKET_NAME,
    competitors_prefix=STR_PREFIX_COMPETITORS,
    client_prefix=STR_PREFIX_CLIENT
)

# 20. Limpieza antes de guardar
canales_a_limpieza = ["Farmacias San Pablo", "Farmacias del Ahorro"]
output_df = output_df[~((output_df["canal"].isin(canales_a_limpieza)) & (output_df["final price"].isna()))]

output_df.drop_duplicates(inplace=True, subset=['canal', 'sku', 'upc', 'price'])

# 21. Agregar columna store id al output
output_df['store id'] = df.set_index(['canal', 'sku', 'upc'])['store id'].reindex(
    output_df.set_index(['canal', 'sku', 'upc']).index
).values

# 22. Asegurar que existan todas las columnas
for col in LST_ORDER_COLUMN:
    if col not in output_df.columns:
        output_df[col] = ""
        log_message(f"Columna agregada: {col}", "WARN")

# 23. Guardar archivo de competidores en S3
log_message("==== GUARDANDO ARCHIVO DE COMPETIDORES ====")

functions_db.save_to_s3(
    output_df,
    STR_BUCKET_NAME,
    STR_PREFIX_COMPETITORS.rstrip('/'),
    FILE_NAME,
    SESSION_S3,
    LST_ORDER_COLUMN
)
log_message(f"Archivo de competidores guardado: {STR_PREFIX_COMPETITORS}{FILE_NAME}")

# 24. Guardar archivo de permanencia en S3
if df_permanencia is not None and len(df_permanencia) > 0:
    log_message("==== GUARDANDO ARCHIVO DE PERMANENCIA ====")

    try:
        permanencia_key = f"{STR_PREFIX_PERMANENCIA}{FILE_NAME_PERMANENCIA}"

        csv_buffer = StringIO()
        df_permanencia.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()

        SESSION_S3.put_object(
            Bucket=STR_BUCKET_NAME,
            Key=permanencia_key,
            Body=csv_content
        )

        log_message(f"Archivo de permanencia guardado: {permanencia_key}")
    except Exception as e:
        log_message(f"Error guardando permanencia: {str(e)}", "ERROR")
else:
    log_message("No se pudo generar DataFrame de permanencia", "WARN")

log_message("==== PROCESO COMPLETADO ====")
print(f"\nResumen:")
print(f"  - Registros procesados: {len(output_df)}")
print(f"  - Archivo competidores: s3://{STR_BUCKET_NAME}/{STR_PREFIX_COMPETITORS}{FILE_NAME}")
if df_permanencia is not None:
    print(f"  - Archivo permanencia: s3://{STR_BUCKET_NAME}/{STR_PREFIX_PERMANENCIA}{FILE_NAME_PERMANENCIA}")

# 25. Verificar cambios de precio en Mounjaro y enviar notificacion
log_message("==== VERIFICANDO CAMBIOS DE PRECIO MOUNJARO ====")
price_change_notifier.run_price_check(df=output_df, send_always=True)

log_message("==== PROCESO FINALIZADO ====")
print("\nProceso terminado")
