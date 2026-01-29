import functions_db 
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
from email_notifications import send_simple_notification

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
OUTPUT_LOCATION = os.getenv('OUTPUT_LOCATION')

LST_CHANNELS = ['elektra', 'liverpool', 'coppel'] 
LST_STORE_IDS = ['9999_elektra_url_bodesa', '9999_liverpool_url_bodesa', '9999_coppel_url_bodesa'] 
LST_ORDER_COLUMN = ['date', 'canal', 'category', 'subcategory', 'subcategory2', 'subcategory3', 'marca', 'modelo', 'sku', 'upc', 'item', 'item characteristics', 'url sku', 'image', 'price', 'sale price', 'shipment cost', 'sales flag', 'store id', 'store name', 'store address', 'stock', 'upc wm2', 'final price', 'upc wm', 'comp', 'precio descuento', 'pago mensualidad', 'pago semanal', 'semanas', 'enganche', 'precio liquidar', 'quincenas']

# Definir fecha límite 
TARGET_DATE = datetime.now()

FILE_NAME = f"bodesa_local_{TARGET_DATE.strftime('%Y-%m-%d')}.csv"
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = 'derivables/bodesa/competitors'
LOG_PREFIX = 'derivables/bodesa/logs'

# Bodesa regex
STR_LIVERPOOL_REGEX = r',(\d+)%\s+[a-zA-Z ]+(\d+)\s+MESES'
STR_ELKETRA_REGEX = r'\$(\d+)\sweekly[_a-zA-Z, ]+(\d+)\s+semanas[a-z-A-Z ]+(\d+)% de enganche'
STR_COPPEL_REGEX = r'\$(\d,*\d+)\s*en\s(\d+)\s*quincenas'

# ============================================
# CONFIGURACIÓN DE EMAIL (Gmail desde .env)
# ============================================
EMAIL_CONFIG = {
    'gmail_user': os.getenv('GMAIL_USER'),
    'gmail_password': os.getenv('GMAIL_PASSWORD'),
    'recipient_emails': os.getenv('ETL_RECIPIENT_EMAILS', '').split(',')
}

# Variable para controlar el status del ETL
etl_status = 'SUCCESS'
log_s3_path = None

try:
    # Inicio del proceso del ETL
    print("Iniciando proceso ETL Bodesa...")
    
    df = functions_db.load_raw_data_from_athena(LST_CHANNELS, LST_STORE_IDS, TARGET_DATE, SESSION_ATHENA)
    print(f"Registros cargados: {df.shape[0]}")

    df = functions_db.clean_competitor_data(df)
    print(f"Registros después de limpieza: {df.shape[0]}")

    # Columnas extra
    df[['percent', 'month']] = df['sales flag'].str.extract(STR_LIVERPOOL_REGEX)
    df[['price', 'final price' ,'percent', 'month']] = df[['price', 'final price' ,'percent', 'month']].astype(float)
    df['precio descuento'] = df['final price'] * (1 -  df['percent']/100)
    df['pago mensualidad'] = df['precio descuento'] / df['month']
    df.drop(columns = ['percent', 'month'], inplace = True)
    df[['pago semanal', 'semanas', 'enganche']] = df['sales flag'].str.extract(STR_ELKETRA_REGEX)
    df[['precio liquidar', 'quincenas']] = df['sales flag'].str.extract(STR_COPPEL_REGEX)
    df['precio liquidar'] = df['precio liquidar'].str.replace(',', '')

    df.fillna('', inplace = True)
    df = df.astype(str)

    # Validación y log
    df, validation_summary = functions_db.validate_and_log_data(
        df=df,
        store_ids_expected=LST_STORE_IDS,
        target_date=TARGET_DATE,
        s3_client=SESSION_S3,
        bucket_name=BUCKET_NAME,
        log_prefix=LOG_PREFIX,
        data_prefix=PREFIX
    )
    
    # Construir ruta del log
    log_filename = f"validation_{TARGET_DATE.strftime('%Y-%m-%d')}.txt"
    log_s3_path = f"{LOG_PREFIX}/{log_filename}"
    
    # Determinar status basado en validaciones
    if validation_summary.get('total_records', 0) == 0:
        etl_status = 'ERROR'
    elif validation_summary.get('missing_stores', 0) > 0:
        etl_status = 'WARNING'

    # Guardar a S3
    functions_db.save_to_s3(df, BUCKET_NAME, PREFIX, FILE_NAME, SESSION_S3, LST_ORDER_COLUMN)
    
    print("Proceso ETL completado exitosamente")

except Exception as e:
    etl_status = 'ERROR'
    print(f"Error en el proceso ETL: {str(e)}")
    
    # Crear validation_summary de emergencia si no existe
    if 'validation_summary' not in locals():
        validation_summary = {
            'error': str(e),
            'status': 'FAILED',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

finally:
    # Enviar notificación por correo (siempre se ejecuta)
    try:
        print("\nEnviando notificación por correo...")
        email_sent = send_simple_notification(
            etl_name='Bodesa Competitors',
            validation_summary=validation_summary,
            log_s3_path=log_s3_path,
            status=etl_status,
            s3_client=SESSION_S3,
            bucket_name=BUCKET_NAME,
            gmail_user=EMAIL_CONFIG['gmail_user'],
            gmail_password=EMAIL_CONFIG['gmail_password'],
            recipient_emails=EMAIL_CONFIG['recipient_emails']
        )
        
        if email_sent:
            print("✓ Notificación enviada exitosamente")
        else:
            print("⚠ No se pudo enviar la notificación")
            
    except Exception as email_error:
        print(f"⚠ Error al enviar notificación: {str(email_error)}")
    
    print("\nProceso terminado")

"""
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


LST_CHANNELS = ['elektra', 'liverpool', 'coppel'] 
LST_STORE_IDS = ['9999_elektra_url_bodesa', '9999_liverpool_url_bodesa', '9999_coppel_url_bodesa'] 
LST_ORDER_COLUMN = ['date', 'canal', 'category', 'subcategory', 'subcategory2', 'subcategory3', 'marca', 'modelo', 'sku', 'upc', 'item', 'item characteristics', 'url sku', 'image', 'price', 'sale price', 'shipment cost', 'sales flag', 'store id', 'store name', 'store address', 'stock', 'upc wm2', 'final price', 'upc wm', 'comp', 'precio descuento', 'pago mensualidad', 'pago semanal', 'semanas', 'enganche', 'precio liquidar', 'quincenas']
# Definir fecha límite 
TARGET_DATE = datetime.now() #- timedelta(days=3) 
# target_date = datetime(2024, 11, 25)  # Fecha específica

FILE_NAME = f"bodesa_local_{TARGET_DATE.strftime('%Y-%m-%d')}.csv"
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = 'derivables/bodesa/competitors'

##Bodesa regex
STR_LIVERPOOL_REGEX = r',(\d+)%\s+[a-zA-Z ]+(\d+)\s+MESES'
STR_ELKETRA_REGEX = r'\$(\d+)\sweekly[_a-zA-Z, ]+(\d+)\s+semanas[a-z-A-Z ]+(\d+)% de enganche'
STR_COPPEL_REGEX = r'\$(\d,*\d+)\s*en\s(\d+)\s*quincenas'

# Inicio del proceso del ETL
df = functions_db.load_raw_data_from_athena(LST_CHANNELS, LST_STORE_IDS, TARGET_DATE, SESSION_ATHENA)
print(df.shape)

df = functions_db.clean_competitor_data(df)
print(df.shape)

#Columnas extra
df[['percent', 'month']] = df['sales flag'].str.extract(STR_LIVERPOOL_REGEX)
df[['price', 'final price' ,'percent', 'month']] = df[['price', 'final price' ,'percent', 'month']].astype(float)
df['precio descuento'] = df['final price'] * (1 -  df['percent']/100)
df['pago mensualidad'] = df['precio descuento'] / df['month']
df.drop(columns = ['percent', 'month'], inplace = True)
df[['pago semanal', 'semanas', 'enganche']] = df['sales flag'].str.extract(STR_ELKETRA_REGEX)
df[['precio liquidar', 'quincenas']] = df['sales flag'].str.extract(STR_COPPEL_REGEX)
df['precio liquidar'] = df['precio liquidar'].str.replace(',', '')

df.fillna('', inplace = True)

df = df.astype(str)

#df = functions_db.get_last_price_from_s3(df, SESSION_S3, BUCKET_NAME, PREFIX)
#print(df.shape)

df, validation_summary = functions_db.validate_and_log_data(
    df=df,
    store_ids_expected=LST_STORE_IDS,
    target_date=TARGET_DATE,
    s3_client=SESSION_S3,
    bucket_name=BUCKET_NAME,
    log_prefix='derivables/bodesa/logs',
    data_prefix=PREFIX
)


functions_db.save_to_s3(df,BUCKET_NAME, PREFIX, FILE_NAME, SESSION_S3, LST_ORDER_COLUMN)

print("Proceso terminado\n")
"""