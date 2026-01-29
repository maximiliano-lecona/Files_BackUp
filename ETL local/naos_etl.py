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

LST_CHANNELS = ['Dermaexpress', 'sanborns', 'heb', 'soriana', 'walmart', 'amazon', 'mercadolibre', 'farmaciasBenavides', 'farmaciasdelahorro', 'farmaciasSanPablo', 'prixz', 'laComer', 'liverpool'] 
LST_STORE_IDS = ['9999_dermaexpress', '9999_sanborns_dermatologicos', '2959_heb_centrodermo', '252_soriana_dermatologicos', '2345_walmart_dermocosmeticos', '9999_amazon_dermo_tiendas_oficiales_allsellers', '9999_mercadolibre_dermo_tiendas_oficiales_allsellers', '9999_benavides_dermocosmeticos', '9999_farmaciasdelahorro_derma', '9999_farmaciassanpablo_dermocosmeticos', '9999_prixz_derma', '287_lacomer_dermatologicosespecializados', '9999_liverpool_cuidadofacial'] #  
LST_ORDER_COLUMN = ['date', 'canal', 'category', 'subcategory', 'subcategory2', 'subcategory3', 'marca', 'modelo', 'sku', 'upc', 'item', 'item characteristics', 'url sku', 'image', 'price', 'sale price', 'shipment cost', 'sales flag', 'store id', 'store name', 'store address', 'stock', 'upc wm2', 'final price', 'upc wm', 'comp', 'last_price']
# Definir fecha límite 
TARGET_DATE = datetime.now() #- timedelta(days=3) 
# target_date = datetime(2024, 11, 25)  # Fecha específica

FILE_NAME = f"naos_test_{TARGET_DATE.strftime('%Y-%m-%d')}.csv"
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = 'derivables/naos/competitors'

# Inicio del proceso del ETL
df = functions_db.load_raw_data_from_athena(LST_CHANNELS, LST_STORE_IDS, TARGET_DATE, SESSION_ATHENA)
print(df.shape)

df = functions_db.clean_competitor_data(df) # LIMPIEZA
print(df.shape)

df.loc[df['store id'] == '9999_farmaciassanpablo_dermocosmeticos', 'canal'] = 'Farmacias San Pablo'
df.loc[df['store id'] == '2959_heb_centrodermo', 'canal'] = 'HEB'
df.loc[df['store id'] == '287_lacomer_dermatologicosespecializados', 'canal'] = 'La Comer'
df.loc[df['store id'] == '9999_sanborns_dermatologicos', 'canal'] = 'Sanborns'
df.loc[df['store id'] == '9999_liverpool_cuidadofacial', 'canal'] = 'Liverpool'
df.loc[df['store id'] == '9999_farmaciasdelahorro_derma', 'canal'] = 'Farmacias del Ahorro'
df.loc[df['store id'] == '9999_prixz_derma', 'canal'] = 'Prixz'
df.loc[df['store id'] == '9999_benavides_dermocosmeticos', 'canal'] = 'Benavides'
df.loc[df['store id'] == '252_soriana_dermatologicos', 'canal'] = 'Soriana'
df.loc[df['store id'] == '9999_dermaexpress', 'canal'] = 'Dermaexpress'
df.loc[df['store id'] == '9999_amazon_dermo_tiendas_oficiales_allsellers', 'canal'] = 'Amazon'
df.loc[df['store id'] == '9999_mercadolibre_urls_dermocosmeticos', 'canal'] = 'Mercadolibre'
df.loc[df['store id'] == '2345_walmart_dermocosmeticos', 'canal'] = 'Walmart'
df['canal'].unique()


df = functions_db.get_last_price_from_s3(df, SESSION_S3, BUCKET_NAME, PREFIX)
print(df.shape)

df, validation_summary = functions_db.validate_and_log_data(
    df=df,
    store_ids_expected=LST_STORE_IDS,
    target_date=TARGET_DATE,
    s3_client=SESSION_S3,
    bucket_name=BUCKET_NAME,
    log_prefix='derivables/naos/logs',
    data_prefix=PREFIX
)


functions_db.save_to_s3(df,BUCKET_NAME, PREFIX, FILE_NAME, SESSION_S3, LST_ORDER_COLUMN)

print("Proceso terminado\n")