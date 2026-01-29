"""
Script para detectar cambios de precio en productos Mounjaro y enviar notificaciones por correo.
Lee el CSV generado por validacion_yza.ipynb y envía alertas cuando hay cambios >= 5%
"""

import smtplib
import pandas as pd
from datetime import datetime
from typing import List
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# ============================================================================
# CONFIGURACIÓN
# ============================================================================
GMAIL_USER = ""
GMAIL_PASSWORD = ""
RECIPIENT_EMAILS = [""] 

# UPCs de Mounjaro a monitorear
UPCS_MOUNJARO = [
    "7501082243741",
    "7501082243727",
    "7501082243710",
    "7501082243734",
    "7501082243642",
    "7501082243635"
]

# Canales a incluir en el reporte
CANALES = ["Benavides", "Farmacias del Ahorro", "Farmacias GDL", "Farmacias San Pablo", "Walmart", "Similares CDMX"]

# Umbral de cambio de precio (%)
THRESHOLD_PERCENT = 5.0

# Logo de Data Bunker
LOGO_URL = "https://logos-amplify-data.s3.us-east-2.amazonaws.com/logo_data_bunker_blanco.png"


# ============================================================================
# FUNCIONES
# ============================================================================

def load_csv(file_path: str) -> pd.DataFrame:
    """Carga el CSV con los tipos de datos correctos"""
    return pd.read_csv(
        file_path,
        dtype={
            'upc': str,
            'upc llave': str,
            'sku': str,
            'upc wm2': str,
            'upc marca prop': str,
            'upc_anterior': str
        },
        encoding='utf-8-sig'
    )


def detect_price_changes(df: pd.DataFrame, upcs: List[str],
                         threshold: float = 5.0) -> pd.DataFrame:
    """
    Detecta productos con cambio de precio >= threshold%

    Returns:
        DataFrame con los productos que tienen cambios significativos
    """
    # Filtrar por UPCs de interés
    df_filtered = df[df["upc"].astype(str).isin(upcs)].copy()

    # Solo considerar filas donde tenemos last_price para comparar
    df_with_history = df_filtered[df_filtered["last_price"].notna()].copy()

    if df_with_history.empty:
        return pd.DataFrame()

    # Calcular el porcentaje de cambio
    df_with_history["price_change_pct"] = (
        (df_with_history["final price"] - df_with_history["last_price"])
        / df_with_history["last_price"] * 100
    ).round(2)

    # Filtrar cambios significativos (absoluto >= threshold)
    df_changes = df_with_history[
        df_with_history["price_change_pct"].abs() >= threshold
    ].copy()

    return df_changes


def get_price_summary(df: pd.DataFrame, upcs: List[str],
                      canales: List[str] = None) -> pd.DataFrame:
    """Obtiene resumen de precios por canal y producto"""
    df_filtered = df[df["upc"].astype(str).isin(upcs)].copy()

    if canales:
        df_filtered = df_filtered[df_filtered["canal"].isin(canales)]

    columns = ["canal", "upc", "item", "image", "price", "sale price", "final price", "last_price", "url sku"]
    available_cols = [c for c in columns if c in df_filtered.columns]

    return df_filtered[available_cols].sort_values(["canal", "item"])


def build_email_html(df_changes: pd.DataFrame, df_summary: pd.DataFrame,
                     product_name: str = "Mounjaro") -> str:
    """Construye el HTML del correo optimizado para Gmail, Outlook y movil"""

    # Filas de cambios detectados
    changes_rows = ""
    if not df_changes.empty:
        for _, row in df_changes.iterrows():
            change_pct = row.get("price_change_pct", 0)
            arrow = "+" if change_pct > 0 else ""
            change_color = "#c0392b" if change_pct > 0 else "#27ae60"
            item_text = str(row['item'])[:40] + "..." if len(str(row['item'])) > 40 else row['item']

            # Link al producto
            url_sku = row.get('url sku', '')
            if pd.notna(url_sku) and url_sku:
                item_cell = f'<a href="{url_sku}" style="color: #2c3e50; text-decoration: underline;">{item_text}</a>'
            else:
                item_cell = item_text

            changes_rows += f"""
            <tr>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e0e0e0; font-family: Arial, sans-serif; font-size: 13px;">{row['canal']}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e0e0e0; font-size: 11px; font-family: Arial, sans-serif;">{item_cell}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e0e0e0; text-align: right; font-family: Arial, sans-serif; font-size: 12px;">${row['last_price']:,.2f}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e0e0e0; text-align: right; font-weight: bold; font-family: Arial, sans-serif; font-size: 12px;">${row['final price']:,.2f}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e0e0e0; text-align: center; color: {change_color}; font-weight: bold; font-family: Arial, sans-serif; font-size: 12px;">
                    {arrow}{change_pct:.1f}%
                </td>
            </tr>
            """

    # Generar tablas por competidor
    competitor_tables = ""
    canales_unicos = df_summary["canal"].unique()

    # Colores para los headers de cada competidor
    colores_competidores = {
        "Benavides": "#c0392b",
        "Farmacias del Ahorro": "#27ae60",
        "Farmacias GDL": "#2980b9",
        "Farmacias San Pablo": "#8e44ad",
        "Walmart": "#f39c12",
        "Similares CDMX": "#16a085"
    }

    for canal in canales_unicos:
        df_canal = df_summary[df_summary["canal"] == canal]
        color_header = colores_competidores.get(canal, "#34495e")

        rows_canal = ""
        for _, row in df_canal.iterrows():
            price_val = f"${row['price']:,.2f}" if pd.notna(row.get('price')) else "-"
            sale_val = f"${row['sale price']:,.2f}" if pd.notna(row.get('sale price')) else "-"
            final_val = f"${row['final price']:,.2f}" if pd.notna(row.get('final price')) else "-"
            last_val = f"${row['last_price']:,.2f}" if pd.notna(row.get('last_price')) else "-"

            item_display = str(row['item'])[:40] + "..." if len(str(row['item'])) > 40 else row['item']

            # Imagen del producto
            image_url = row.get('image', '')
            if pd.notna(image_url) and image_url:
                img_cell = f'<img src="{image_url}" alt="" style="width: 40px; height: 40px; object-fit: contain; border: 1px solid #eeeeee;" width="40" height="40">'
            else:
                img_cell = '<div style="width: 40px; height: 40px; background-color: #f0f0f0;"></div>'

            # Link al producto
            url_sku = row.get('url sku', '')
            if pd.notna(url_sku) and url_sku:
                item_cell = f'<a href="{url_sku}" style="color: #2c3e50; text-decoration: underline;">{item_display}</a>'
            else:
                item_cell = item_display

            rows_canal += f"""
            <tr>
                <td style="padding: 6px; border-bottom: 1px solid #eeeeee; text-align: center; vertical-align: middle;">{img_cell}</td>
                <td style="padding: 8px 6px; border-bottom: 1px solid #eeeeee; font-size: 11px; font-family: Arial, sans-serif; vertical-align: middle;">{item_cell}</td>
                <td style="padding: 8px 6px; border-bottom: 1px solid #eeeeee; text-align: right; font-family: Arial, sans-serif; font-size: 11px; vertical-align: middle;">{price_val}</td>
                <td style="padding: 8px 6px; border-bottom: 1px solid #eeeeee; text-align: right; color: #e67e22; font-family: Arial, sans-serif; font-size: 11px; vertical-align: middle;">{sale_val}</td>
                <td style="padding: 8px 6px; border-bottom: 1px solid #eeeeee; text-align: right; font-weight: bold; color: #2c3e50; font-family: Arial, sans-serif; font-size: 11px; vertical-align: middle;">{final_val}</td>
                <td style="padding: 8px 6px; border-bottom: 1px solid #eeeeee; text-align: right; color: #7f8c8d; font-family: Arial, sans-serif; font-size: 11px; vertical-align: middle;">{last_val}</td>
            </tr>
            """

        competitor_tables += f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; margin-bottom: 15px; border: 1px solid #e0e0e0;">
            <tr>
                <td colspan="6" style="background-color: {color_header}; color: #ffffff; padding: 10px 12px; font-family: Arial, sans-serif; font-size: 14px; font-weight: bold;">
                    {canal}
                </td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <th style="padding: 8px 6px; text-align: center; font-weight: bold; color: #333333; font-family: Arial, sans-serif; font-size: 10px; border-bottom: 2px solid #e0e0e0; width: 50px;"></th>
                <th style="padding: 8px 6px; text-align: left; font-weight: bold; color: #333333; font-family: Arial, sans-serif; font-size: 10px; border-bottom: 2px solid #e0e0e0;">Producto</th>
                <th style="padding: 8px 6px; text-align: right; font-weight: bold; color: #333333; font-family: Arial, sans-serif; font-size: 10px; border-bottom: 2px solid #e0e0e0;">Precio</th>
                <th style="padding: 8px 6px; text-align: right; font-weight: bold; color: #e67e22; font-family: Arial, sans-serif; font-size: 10px; border-bottom: 2px solid #e0e0e0;">Oferta</th>
                <th style="padding: 8px 6px; text-align: right; font-weight: bold; color: #2c3e50; font-family: Arial, sans-serif; font-size: 10px; border-bottom: 2px solid #e0e0e0;">Final</th>
                <th style="padding: 8px 6px; text-align: right; font-weight: bold; color: #7f8c8d; font-family: Arial, sans-serif; font-size: 10px; border-bottom: 2px solid #e0e0e0;">Anterior</th>
            </tr>
            {rows_canal}
        </table>
        """

    num_changes = len(df_changes)

    # Sección de alertas (solo si hay cambios)
    alert_section = ""
    if num_changes > 0:
        alert_section = f"""
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #e74c3c; margin-bottom: 20px;">
            <tr>
                <td style="padding: 15px; color: #ffffff; font-family: Arial, sans-serif;">
                    <p style="margin: 0 0 8px 0; font-size: 18px; font-weight: bold;">Se detectaron {num_changes} cambio(s) de precio significativo(s)</p>
                    <p style="margin: 0; font-size: 13px;">Los siguientes productos tuvieron un cambio de precio mayor o igual al 5%</p>
                </td>
            </tr>
        </table>

        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; margin-bottom: 25px; border: 1px solid #e0e0e0;">
            <tr style="background-color: #5b4b8a;">
                <td style="color: #ffffff; padding: 10px 8px; font-family: Arial, sans-serif; font-weight: bold; font-size: 11px;">Canal</td>
                <td style="color: #ffffff; padding: 10px 8px; font-family: Arial, sans-serif; font-weight: bold; font-size: 11px;">Producto</td>
                <td style="color: #ffffff; padding: 10px 8px; text-align: right; font-family: Arial, sans-serif; font-weight: bold; font-size: 11px;">Anterior</td>
                <td style="color: #ffffff; padding: 10px 8px; text-align: right; font-family: Arial, sans-serif; font-weight: bold; font-size: 11px;">Actual</td>
                <td style="color: #ffffff; padding: 10px 8px; text-align: center; font-family: Arial, sans-serif; font-weight: bold; font-size: 11px;">Cambio</td>
            </tr>
            {changes_rows}
        </table>
        """

    html = f"""
    <!DOCTYPE html>
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <title>Monitor de Precios - {product_name}</title>
        <!--[if mso]>
        <noscript>
        <xml>
            <o:OfficeDocumentSettings>
                <o:PixelsPerInch>96</o:PixelsPerInch>
            </o:OfficeDocumentSettings>
        </xml>
        </noscript>
        <![endif]-->
        <style type="text/css">
            @media only screen and (max-width: 600px) {{
                .email-container {{
                    width: 100% !important;
                }}
                .responsive-table {{
                    width: 100% !important;
                }}
                .mobile-padding {{
                    padding: 10px !important;
                }}
            }}
        </style>
    </head>
    <body style="margin: 0; padding: 0; background-color: #f5f7fa; font-family: Arial, sans-serif; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f5f7fa;">
            <tr>
                <td align="center" style="padding: 10px;">
                    <!--[if mso]>
                    <table width="700" cellpadding="0" cellspacing="0" border="0">
                    <tr><td>
                    <![endif]-->
                    <table class="email-container" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width: 700px;">

                        <!-- Header con Logo -->
                        <tr>
                            <td style="background-color: #1a1a2e; color: #ffffff; padding: 25px 15px; text-align: center;">
                                <img src="{LOGO_URL}" alt="Data Bunker" style="height: 40px; margin-bottom: 12px;" height="40">
                                <h1 style="margin: 0 0 8px 0; font-size: 22px; font-weight: bold; font-family: Arial, sans-serif;">Monitor de Precios</h1>
                                <p style="margin: 0; font-size: 14px; color: #cccccc; font-family: Arial, sans-serif;">{product_name} - Reporte de Competidores</p>
                            </td>
                        </tr>

                        <!-- Content -->
                        <tr>
                            <td class="mobile-padding" style="background-color: #f8f9fa; padding: 20px 15px;">

                                {alert_section}

                                <!-- Titulo de seccion de competidores -->
                                <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 15px;">
                                    <tr>
                                        <td>
                                            <h3 style="margin: 0 0 5px 0; font-size: 16px; color: #2c3e50; font-family: Arial, sans-serif;">Precios por Competidor</h3>
                                            <p style="margin: 0; font-size: 12px; color: #7f8c8d; font-family: Arial, sans-serif;">Resumen de precios actuales de {product_name} en cada tienda</p>
                                        </td>
                                    </tr>
                                </table>

                                <!-- Tablas por competidor -->
                                {competitor_tables}

                            </td>
                        </tr>

                        <!-- Footer con fondo oscuro para el logo -->
                        <tr>
                            <td style="background-color: #1a1a2e; padding: 20px 15px; text-align: center;">
                                <img src="{LOGO_URL}" alt="Data Bunker" style="height: 25px; margin-bottom: 10px;" height="25">
                                <p style="color: #999999; font-size: 11px; margin: 0; font-family: Arial, sans-serif;">
                                    Este correo fue generado automaticamente por el sistema de monitoreo de precios.
                                </p>
                                <p style="color: #666666; font-size: 10px; margin: 5px 0 0 0; font-family: Arial, sans-serif;">
                                    Data Bunker {datetime.now().year}
                                </p>
                            </td>
                        </tr>

                    </table>
                    <!--[if mso]>
                    </td></tr></table>
                    <![endif]-->
                </td>
            </tr>
        </table>
    </body>
    </html>
    """

    return html


def send_email(subject: str, html_body: str, gmail_user: str,
               gmail_password: str, recipients: List[str]) -> bool:
    """Envía el correo via Gmail SMTP"""
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = gmail_user
        msg['To'] = ', '.join(recipients)

        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(gmail_user, gmail_password)
            server.send_message(msg)

        print(f"✓ Correo enviado exitosamente a {', '.join(recipients)}")
        return True

    except Exception as e:
        print(f"✗ Error al enviar correo: {str(e)}")
        return False


def run_price_check(csv_path: str, send_always: bool = False):
    """
    Función principal que ejecuta todo el proceso

    Args:
        csv_path: Ruta al archivo CSV
        send_always: Si True, envía correo aunque no haya cambios significativos
    """
    print(f"📂 Cargando archivo: {csv_path}")
    df = load_csv(csv_path)
    print(f"   Registros cargados: {len(df):,}")

    # Detectar cambios de precio
    print(f"\n🔍 Buscando cambios de precio >= {THRESHOLD_PERCENT}%...")
    df_changes = detect_price_changes(df, UPCS_MOUNJARO, THRESHOLD_PERCENT)

    # Obtener resumen completo
    df_summary = get_price_summary(df, UPCS_MOUNJARO, CANALES)
    print(f"   Productos en resumen: {len(df_summary)}")

    if df_changes.empty and not send_always:
        print(f"\n✅ No se detectaron cambios de precio >= {THRESHOLD_PERCENT}%")
        print("   No se enviará correo.")
        return False

    if df_changes.empty:
        print(f"\nℹ️ No hay cambios significativos, pero se enviará el resumen (send_always=True)")
    else:
        print(f"\n⚠️ Se detectaron {len(df_changes)} producto(s) con cambio >= {THRESHOLD_PERCENT}%:")
        for _, row in df_changes.iterrows():
            print(f"   • {row['canal']} | {row['item'][:40]}... | {row['price_change_pct']:+.1f}%")

    # Construir y enviar correo
    print(f"\n📧 Preparando correo...")
    subject = f"Alerta de Cambio de Precios - Mounjaro - {datetime.now().strftime('%Y-%m-%d')}"
    html_body = build_email_html(df_changes, df_summary, "Mounjaro")

    return send_email(subject, html_body, GMAIL_USER, GMAIL_PASSWORD, RECIPIENT_EMAILS)


# ============================================================================
# EJECUCIÓN
# ============================================================================
if __name__ == "__main__":
    # Ruta al CSV (ajustar según necesidad)
    csv_path = f""

    # Ejecutar verificación
    # send_always=True para enviar aunque no haya cambios
    run_price_check(csv_path, send_always=False)
