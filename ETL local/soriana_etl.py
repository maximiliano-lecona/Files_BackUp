import functions_db 
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime

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

# Configuración de salida
OUTPUT_LOCATION =os.getenv('OUTPUT_LOCATION')


LST_CHANNELS = ['farmaciasGDL', 'farmaciasBenavides', 'farmaciasYZA', 'farmaciasroma',  'farmaciasdelahorro']  # 'walmart',
LST_STORE_IDS = ['44100_farmaciasgdl', '9999_benavides', '9999_farmaciasyza', '9999_farmaciasroma',  '9999_farmaciasdelahorro_promos', '9999_benavides_promos', '9999_farmaciasdelahorro'] # '2345_walmart_farmacia_subcategory', , '9999_farmaciasyzacuidatemucho'
LST_ORDER_COLUMN = ['date', 'canal', 'category', 'subcategory', 'subcategory2', 'subcategory3', 'marca', 'modelo', 'sku', 'upc', 'item', 'item characteristics', 'url sku', 'image', 'price', 'sale price', 'shipment cost', 'sales flag', 'store id', 'store name', 'store address', 'stock', 'upc wm2', 'final price', 'upc wm', 'comp', 'last_price' ]
# Definir fecha límite 
TARGET_DATE = datetime.now() #- timedelta(days=3) 
# target_date = datetime(2024, 11, 25)  # Fecha específica

FILE_NAME = f"soriana_local_{TARGET_DATE.strftime('%Y-%m-%d')}.csv"
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = 'derivables/soriana/competitors_online'

# Inicio del proceso del ETL
df = functions_db.load_raw_data_from_athena(LST_CHANNELS, LST_STORE_IDS, TARGET_DATE, SESSION_ATHENA)
print(df.shape)

df = functions_db.clean_competitor_data(df)
print(df.shape)


df = functions_db.get_last_price_from_s3(df, SESSION_S3, BUCKET_NAME, PREFIX)
#df["last_price"] = ""

"""
df, validation_summary = functions_db.validate_and_log_data(
    df=df,
    store_ids_expected=LST_STORE_IDS,
    target_date=TARGET_DATE,
    s3_client=SESSION_S3,
    bucket_name=BUCKET_NAME,
    log_prefix='derivables/soriana/logs',
    data_prefix=PREFIX
)
"""

strToday = TARGET_DATE.strftime('%Y-%m-%d')
df['date'] = strToday

functions_db.save_to_s3(df,BUCKET_NAME, PREFIX, FILE_NAME, SESSION_S3, LST_ORDER_COLUMN)

print("Proceso terminado\n")