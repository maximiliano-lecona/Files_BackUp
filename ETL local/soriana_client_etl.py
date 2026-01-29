import functions_db
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime, timedelta

# =========================
# CONFIGURACIÓN
# =========================
load_dotenv()

SESSION_ATHENA = boto3.Session(
    region_name=os.getenv('AWS_DEFAULT_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

SESSION_S3 = boto3.client(
    's3',
    region_name=os.getenv('AWS_DEFAULT_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

OUTPUT_LOCATION = os.getenv('OUTPUT_LOCATION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

LST_CHANNELS = ['soriana']
LST_STORE_IDS = ['252_soriana_farmacia']
PREFIX = 'derivables/soriana/client'

TARGET_DATE = datetime.now() #- timedelta(days=2) 
FILE_NAME = f"client_{TARGET_DATE.strftime('%Y-%m-%d')}.csv"


LST_COLUMN_CLIENT = ['departamento', '# departamento', 'categoria',	'# categoria', 'subcategoria', '# subcategoría', 'sku',
                                 'descripción sku',	'ean',	'marca', 'precio sin promocion sin iva', 'precio sin promocion con iva',	'precio sin iva','precio con iva',	'iva',	'operable',	'activo', 'status',	'direccion comercial',	'proveedor', 'comprador', 'inventario', 'margen',	'margen min', 'grupo', 'pmp con iva', '$ ventas ytd', 'ventas pzas ytd', '$ ventas mtd', 'ventas pzas mtd',	'mercado mtd pzas',	'mercado ytd pzas',	'mercado mtd $',	'mercado ytd $', 'ean wm'] 

DICT_COLUMN_CLIENT = {'category': 'departamento','subcategory': 'categoria','subcategory2': 'subcategoria','sku': 'sku','item': 'descripcion sku','upc': 'ean','upc wm':'ean wm','marca': 'marca','price': 'precio sin promocion con iva','final price': 'precio con iva','sales flag': 'status','date': 'proveedor'}

print("⏳ Cargando datos desde Athena...")
df = functions_db.load_raw_data_from_athena(
    LST_CHANNELS,
    LST_STORE_IDS,
    TARGET_DATE,
    SESSION_ATHENA
)
print("Shape inicial:", df.shape)

print("🧹 Limpieza de datos...")
df = functions_db.clean_competitor_data(df)
print("Shape post-limpieza:", df.shape)

# Fecha homogénea
df['date'] = TARGET_DATE.strftime('%Y-%m-%d')

# =========================
# CREACIÓN CLIENT (SEGÚN TABLA)
# =========================

# Copia defensiva
df_client = df.copy()

# Asegurar columnas en minúsculas (por si acaso)
df_client.columns = df_client.columns.str.lower()

# Renombrar solo lo definido por la tabla
df_client.rename(columns=DICT_COLUMN_CLIENT, inplace=True)

# Forzar estructura final (crea columnas vacías automáticamente)
df_client = df_client.reindex(columns=LST_COLUMN_CLIENT)

# Rellenar vacíos como en Glue
df_client.fillna("", inplace=True)

# Reemplazar df final
df = df_client


print("Shape final CLIENT:", df.shape)

# =========================
# GUARDAR EN S3
# =========================
print("☁️ Guardando archivo en S3...")
functions_db.save_to_s3(
    df,
    BUCKET_NAME,
    PREFIX,
    FILE_NAME,
    SESSION_S3,
    LST_COLUMN_CLIENT
)

print("✅ Proceso terminado correctamente\n")
