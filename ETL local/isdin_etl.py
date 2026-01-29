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

LST_CHANNELS = ['amazon', 'farmaciasdelahorro', 'mercadolibre', 'farmaciasSanPablo', 'laComer', 'heb', 'costco', 'walmart', 'farmaciasYZA', 'farmaciasBenavides']
LST_STORE_IDS = ['9999_amazon_dermo_tiendas_oficiales', '9999_farmaciasdelahorro_derma', '9999_farmaciassanpablo_dermocosmeticos', '287_lacomer_dermatologicosespecializados','2959_heb_centrodermo', '9999_costco_dermocosmeticos', '2345_walmart_dermocosmeticos', '9999_mercadolibre_dermo_tiendas_oficiales', '9999_farmaciasyza_dermocosmeticos', '9999_benavides_dermocosmeticos', '9999_farmaciasdelahorro_bexident','9999_farmaciassanpablo_cuidadobucal','287_lacomer_farmacuidadopersonal','9999_costco_cuidadobucal','9999_farmaciasyza_higienebucal','9999_benavides_odontologiahigienebucal']

#LST_CHANNELS = ['farmaciasGDL','chedraui','soriana','liverpool','sanborns', 'amazon', 'farmaciasdelahorro', 'mercadolibre',  'farmaciasSanPablo', 'laComer', 'heb', 'costco', 'walmart', 'mercadolibre', 'farmaciasYZA', 'farmaciasBenavides']
#LST_STORE_IDS = ['44100_farmaciasgdl_dermatologia', '9999_chedraui_dermatologicos', '252_soriana_dermatologicos', '9999_liverpool_cuidadofacial','9999_sanborns_dermatologicos','44100_farmaciasgdl_cuidadobucal','9999_chedraui_higienebucal','252_soriana_cuidadobucal',      '9999_amazon_dermo_tiendas_oficiales', '9999_farmaciasdelahorro_derma', '9999_farmaciassanpablo_dermocosmeticos', '287_lacomer_dermatologicosespecializados','2959_heb_centrodermo', '9999_costco_dermocosmeticos', '2345_walmart_dermocosmeticos', '9999_mercadolibre_dermo_tiendas_oficiales', '9999_farmaciasyza_dermocosmeticos', '9999_benavides_dermocosmeticos', '9999_farmaciasdelahorro_bexident','9999_farmaciassanpablo_cuidadobucal','287_lacomer_farmacuidadopersonal','9999_costco_cuidadobucal','9999_farmaciasyza_higienebucal','9999_benavides_odontologiahigienebucal']

LST_ORDER_COLUMN = ['date', 'canal', 'category', 'subcategory', 'subcategory2', 'subcategory3', 'marca', 'modelo', 'sku', 'upc', 'item', 'item characteristics', 'url sku', 'image', 'price', 'sale price', 'shipment cost', 'sales flag', 'store id', 'store name', 'store address', 'stock', 'upc wm2', 'final price', 'upc wm', 'comp', 'last_price' ]

# Definir fecha límite 
TARGET_DATE = datetime.now() #- timedelta(days=3) 
# target_date = datetime(2024, 11, 25)  # Fecha específica

FILE_NAME = f"isdin_local_{TARGET_DATE.strftime('%Y-%m-%d')}.csv"
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = 'derivables/isdin/competitors'

# Inicio del proceso del ETL
df = functions_db.load_raw_data_from_athena(LST_CHANNELS, LST_STORE_IDS, TARGET_DATE, SESSION_ATHENA)
print(df.shape)

df = functions_db.clean_competitor_data(df)
print(df.shape)

#df['date'] = TARGET_DATE

# Logica del cliente LIMPIEZA
mapeo_canales = {
    "Benavides - Online": "Benavides",
    "Chedraui - Online": "Chedraui",
    "Costco - Online": "Costco",
    "Dermaexpress": "Dermaexpress",
    "Farmacias del Ahorro": "Farmacias del Ahorro",
    "Farmacias del Ahorro - Online": "Farmacias del Ahorro",
    "Farmacias GDL - Online": "Farmacias GDL",
    "Farmacias San Pablo": "Farmacias San Pablo",
    "Farmacias San Pablo - Online": "Farmacias San Pablo",
    "Farmacias Yza - Online": "Farmacias Yza",
    "HEB": "HEB",
    "HEB - GONZALITOS": "HEB",
    "La Comer": "La Comer",
    'La Comer  - Coyoacán': "La Comer",
    "Liverpool": "Liverpool",
    "Mercadolibre": "Mercadolibre",
    "Prixz": "Prixz",
    "Sanborns": "Sanborns",
    "Sanborns - Online": "Sanborns",
    "Soriana": "Soriana",
    "Soriana - MIYANA": "Soriana",
    "Walmart": "Walmart"
}

df['canal'] = df['canal'].map(mapeo_canales).fillna(df['canal'])

df = functions_db.get_last_price_from_s3(df, SESSION_S3, BUCKET_NAME, PREFIX)
print(df.shape)

df, validation_summary = functions_db.validate_and_log_data(
    df=df,
    store_ids_expected=LST_STORE_IDS,
    target_date=TARGET_DATE,
    s3_client=SESSION_S3,
    bucket_name=BUCKET_NAME,
    log_prefix='derivables/isdin/logs',
    data_prefix=PREFIX
)


functions_db.save_to_s3(df,BUCKET_NAME, PREFIX, FILE_NAME, SESSION_S3, LST_ORDER_COLUMN)

print("Proceso terminado\n")