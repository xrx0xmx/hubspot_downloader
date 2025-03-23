"""
Módulo para procesar y guardar emails de HubSpot.
"""

import logging
import pandas as pd
import os
import sys
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import dateutil.parser

from hubspot.api import get_email_content, get_company, is_valid_id
from hubspot.utils import sanitize_filename, get_email_filename, ensure_directory
from hubspot.summarizer import process_email_file

logger = logging.getLogger(__name__)

def format_date(date_value):
    """Formatea una fecha desde diferentes formatos a una cadena legible.
    
    Args:
        date_value: Valor de fecha (timestamp en ms, cadena ISO, etc.)
        
    Returns:
        Cadena de fecha formateada
    """
    try:
        if date_value is None:
            return "Unknown"
        
        # Si es un string que parece una fecha ISO
        if isinstance(date_value, str) and ('T' in date_value or '-' in date_value):
            try:
                # Intentar parsear como fecha ISO
                dt = dateutil.parser.parse(date_value)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logger.debug(f"Error parsing ISO date: {e}")
        
        # Si es un número (timestamp en ms)
        try:
            # Convertir a entero y dividir por 1000 si es necesario
            timestamp = int(float(date_value))
            # Si el timestamp es muy grande (ms), convertir a segundos
            if timestamp > 1000000000000:  # Probablemente en milisegundos
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.debug(f"Error parsing timestamp: {e}")
        
        # Si todo falla, devolver el valor como string
        return str(date_value)
    except Exception as e:
        logger.error(f"Error formatting date {date_value}: {e}")
        return "Unknown"

def save_email_content(email_data: Dict, contact_info: Dict, company_info: Dict, base_dir: str, force: bool = False, summarize: bool = False) -> bool:
    """Guarda el contenido del email en una estructura organizada por empresa y contacto.
    
    Args:
        email_data: Datos del email
        contact_info: Información del contacto
        company_info: Información de la empresa
        base_dir: Directorio base para guardar los emails
        force: Si se debe forzar la descarga aunque el archivo exista
        summarize: Si se debe generar un resumen del email usando OpenAI
        
    Returns:
        True si se guardó el email, False si se omitió
    """
    try:
        email_id = email_data.get('id', 'unknown')
        properties = email_data.get('properties', {})
        
        # Obtener dirección de correo del remitente
        from_email_address = properties.get('hs_email_from_email', '')
        
        # Obtener dirección de correo del destinatario
        to_email_address = properties.get('hs_email_to_email', '')
        if not to_email_address:
            to_email_address = contact_info.get('email', '')
        
        # Verificar si el email proviene de bondo.es
        is_from_bondo = from_email_address and '@bondo.es' in from_email_address.lower()
        
        # Determinar la estructura de directorios según el origen del email
        if is_from_bondo and to_email_address:
            # Si el email viene de bondo.es, guardar en el directorio del destinatario
            logger.info(f"Email de bondo.es detectado, guardando en directorio del destinatario: {to_email_address}")
            contact_folder = sanitize_filename(to_email_address)
            email_dir = ensure_directory(Path(base_dir) / contact_folder)
        else:
            # Estructura normal organizada por empresa y contacto
            # Obtener nombres sanitizados para las carpetas
            company_name = sanitize_filename(company_info.get('properties', {}).get('name', 'unknown_company'))
            
            # Usar email como nombre de carpeta si está disponible, de lo contrario usar nombre del contacto
            if to_email_address and to_email_address.strip():
                contact_folder = sanitize_filename(to_email_address)
            else:
                contact_name = sanitize_filename(f"{contact_info.get('firstname', '')} {contact_info.get('lastname', '')}".strip())
                contact_folder = contact_name if contact_name else 'unknown_contact'

            # Crear estructura de directorios
            email_dir = ensure_directory(Path(base_dir) / company_name / contact_folder)

        # Generar nombre de archivo
        filename = get_email_filename(properties, email_id)
        file_path = email_dir / filename

        # Verificar si el archivo ya existe
        if file_path.exists() and not force:
            logger.debug(f"Email {email_id} already exists at {file_path}, skipping...")
            # Si se solicita resumir y el archivo existe, intentar resumirlo
            if summarize:
                process_email_file(file_path)
            return False

        # Guardar contenido del email
        if text_content := properties.get('hs_email_text'):
            # Mensaje destacado para el guardado de email
            subject = properties.get('hs_email_subject', 'Sin asunto')
            logger.info(f"📥 GUARDANDO EMAIL | ID: {email_id} | Asunto: '{subject}' | Ruta: {file_path}")
            
            # Formatear la fecha correctamente
            email_date = format_date(properties.get('hs_timestamp'))
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"Subject: {properties.get('hs_email_subject', '')}\n")
                f.write(f"From: {properties.get('hs_email_from_email', '')}\n")
                f.write(f"To: {properties.get('hs_email_to_email', '')}\n")
                f.write(f"Date: {email_date}\n")
                f.write(f"Email ID: {email_id}\n")
                f.write("\n" + "="*50 + "\n\n")
                f.write(text_content)
                f.flush()
                os.fsync(f.fileno())
            
            logger.info(f"✅ EMAIL GUARDADO | ID: {email_id} | Archivo: {file_path}")
            
            # Generar resumen si se solicita
            if summarize:
                process_email_file(file_path)
                
            return True
        else:
            logger.warning(f"❌ El email {email_id} no tiene contenido de texto")

    except Exception as e:
        logger.error(f"Error saving email content for ID {email_id}: {e}")
    return False

def download_email_contents(emails_csv: str, output_dir: str, force: bool = False, summarize: bool = False) -> None:
    """Descarga y guarda el contenido completo de todos los emails organizados por empresa y contacto.
    
    Args:
        emails_csv: Ruta al archivo CSV con los emails
        output_dir: Directorio de salida para los emails
        force: Si se debe forzar la descarga aunque el archivo exista
        summarize: Si se debe generar un resumen del email usando OpenAI
    """
    try:
        # Crear directorio base si no existe
        ensure_directory(output_dir)
        logger.info(f"Directorio de salida: {os.path.abspath(output_dir)}")
        
        # Cargar datos
        logger.info(f"Cargando datos desde {emails_csv}...")
        emails_df = pd.read_csv(emails_csv)
        contacts_df = pd.read_csv('hubspot_contacts.csv')
        
        # Crear diccionarios para búsqueda rápida
        contacts_dict = {}
        companies_dict = {}
        
        logger.info("Loading contact and company information...")
        for _, contact in contacts_df.iterrows():
            contact_id = contact.get('id')
            if contact_id:
                contacts_dict[str(contact_id)] = contact.to_dict()
                
                # Obtener información de la empresa si no la tenemos ya
                company_id = contact.get('properties.associatedcompanyid')
                if is_valid_id(company_id) and str(company_id) not in companies_dict:
                    try:
                        # Asegurarse de que el company_id sea un string
                        company_id_str = str(company_id)
                        # Ahora get_company primero intentará cargar desde disco
                        company_info = get_company(company_id_str)
                        if company_info:
                            companies_dict[company_id_str] = company_info
                    except Exception as e:
                        logger.error(f"Error processing company ID {company_id}: {e}")

        # Procesar emails
        total_emails = len(emails_df)
        processed_emails = 0
        skipped_emails = 0
        error_emails = 0
        logger.info(f"Starting download of {total_emails} email contents...")
        logger.info(f"Los emails se guardarán en: {os.path.abspath(output_dir)}")
        logger.info(f"NOTA: Cada email se guardará inmediatamente después de obtenerlo de la API")
        logger.info(f"NOTA: La información de empresas se cargará desde disco si está disponible")

        for idx, row in emails_df.iterrows():
            try:
                # Mostrar progreso más frecuentemente
                if idx % 10 == 0:
                    logger.info(f"Processing email {idx + 1} of {total_emails}...")
                    # Forzar la salida del log
                    sys.stdout.flush()
                
                email_id = row.get('properties.hs_object_id')
                if not is_valid_id(email_id):
                    logger.debug(f"Skipping email with invalid ID: {email_id}")
                    error_emails += 1
                    continue

                # Obtener el contacto asociado al email
                # Corregir el error 'float' object has no attribute 'lower'
                to_email_raw = row.get('properties.hs_email_to_email', '')
                # Asegurarse de que to_email sea una cadena de texto
                to_email = str(to_email_raw).lower() if to_email_raw is not None else ''
                
                # También asegurar que el email del contacto sea una cadena
                contact_info = next(
                    (contact for contact in contacts_dict.values() 
                     if str(contact.get('properties.email', '')).lower() == to_email),
                    {'firstname': '', 'lastname': '', 'email': to_email}
                )
                
                # Obtener la empresa asociada al contacto
                company_id = contact_info.get('properties.associatedcompanyid')
                company_info = {}
                if is_valid_id(company_id):
                    try:
                        company_id_str = str(company_id)
                        company_info = companies_dict.get(company_id_str, {})
                    except Exception as e:
                        logger.error(f"Error getting company info for ID {company_id}: {e}")

                # Obtener y guardar el contenido del email
                try:
                    email_id_str = str(email_id)
                    logger.info(f"Procesando email {idx + 1}/{total_emails}: ID={email_id_str}")
                    
                    # Obtener el contenido del email de la API
                    email_data = get_email_content(email_id_str)
                    
                    if email_data:
                        # Preparar información para el guardado
                        properties = email_data.get('properties', {})
                        subject = properties.get('hs_email_subject', 'Sin asunto')
                        
                        # Obtener nombres sanitizados para las carpetas
                        company_name = sanitize_filename(company_info.get('properties', {}).get('name', 'unknown_company'))
                        to_email_address = properties.get('hs_email_to_email', '')
                        if not to_email_address:
                            to_email_address = contact_info.get('email', '')
                        
                        # Usar email como nombre de carpeta si está disponible, de lo contrario usar nombre del contacto
                        if to_email_address and to_email_address.strip():
                            contact_folder = sanitize_filename(to_email_address)
                        else:
                            contact_name = sanitize_filename(f"{contact_info.get('firstname', '')} {contact_info.get('lastname', '')}".strip())
                            contact_folder = contact_name if contact_name else 'unknown_contact'
                        
                        # Generar nombre de archivo
                        filename = get_email_filename(properties, email_id_str)
                        email_path = os.path.join(output_dir, company_name, contact_folder, filename)
                        
                        # Guardar inmediatamente el email en disco
                        logger.info(f"Guardando email {email_id_str} en disco: {os.path.abspath(email_path)}")
                        if save_email_content(email_data, contact_info, company_info, output_dir, force, summarize):
                            processed_emails += 1
                            logger.info(f"Email {email_id_str} guardado correctamente ({processed_emails} emails guardados hasta ahora)")
                            
                            # Forzar la salida del log
                            sys.stdout.flush()
                        else:
                            skipped_emails += 1
                            logger.info(f"Email {email_id_str} omitido (ya existe o sin contenido)")
                    else:
                        error_emails += 1
                        logger.warning(f"No se pudo obtener contenido para el email {email_id_str}")
                except Exception as e:
                    logger.error(f"Error processing email ID {email_id}: {e}")
                    error_emails += 1
            except Exception as e:
                logger.error(f"Error procesando email {idx + 1}: {e}")
                error_emails += 1
                continue

            # Mostrar estadísticas parciales cada 20 emails
            if (idx + 1) % 20 == 0 or (idx + 1) == total_emails:
                logger.info(f"=== ESTADÍSTICAS PARCIALES ({idx + 1}/{total_emails}) ===")
                logger.info(f"- Nuevos emails guardados: {processed_emails}")
                logger.info(f"- Emails omitidos: {skipped_emails}")
                logger.info(f"- Emails con errores: {error_emails}")
                logger.info(f"- Directorio de salida: {os.path.abspath(output_dir)}")
                logger.info(f"=======================================")
                
                # Forzar la salida del log
                sys.stdout.flush()

        logger.info(f"Email download completed:")
        logger.info(f"- Total emails processed: {total_emails}")
        logger.info(f"- New emails downloaded: {processed_emails}")
        logger.info(f"- Existing emails skipped: {skipped_emails}")
        logger.info(f"- Emails with errors: {error_emails}")
        logger.info(f"- Output directory: {os.path.abspath(output_dir)}")
        logger.info(f"- Summarization enabled: {summarize}")

    except Exception as e:
        logger.error(f"Error in email content download process: {e}")
        raise
