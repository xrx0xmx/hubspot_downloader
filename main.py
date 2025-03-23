#!/usr/bin/env python3
"""
Script principal para descargar datos de HubSpot.

Este script descarga contactos, engagements y contenido de emails de HubSpot,
respetando los límites de la API (110 peticiones cada 10 segundos).
"""

import logging
import argparse
from pathlib import Path

from hubspot.api import get_contacts, get_engagements
from hubspot.models import save_to_csv
from hubspot.email_processor import download_email_contents
from hubspot.summarizer import process_emails_in_directory

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reducir el nivel de logging de las bibliotecas externas
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

def parse_args():
    """Parsea argumentos de línea de comando.
    
    Returns:
        Argumentos parseados
    """
    parser = argparse.ArgumentParser(description='Descargar datos de HubSpot')
    parser.add_argument('--force', action='store_true',
                      help='Forzar la descarga de emails aunque ya existan localmente')
    parser.add_argument('--output-dir', type=str, default='email_contents',
                      help='Directorio donde guardar los emails (default: email_contents)')
    parser.add_argument('--skip-contacts', action='store_true',
                      help='Omitir la descarga de contactos')
    parser.add_argument('--skip-engagements', action='store_true',
                      help='Omitir la descarga de engagements')
    parser.add_argument('--skip-emails', action='store_true',
                      help='Omitir la descarga de contenido de emails')
    parser.add_argument('--summarize', action='store_true',
                      help='Generar resúmenes de emails usando OpenAI')
    parser.add_argument('--only-summarize', action='store_true',
                      help='Solo procesar emails existentes para generar resúmenes (no descargar nuevos)')
    parser.add_argument('--verbose', '-v', action='store_true',
                      help='Mostrar información detallada durante la ejecución')
    return parser.parse_args()

def main():
    """Función principal del script.
    
    Este script respeta los límites de la API de HubSpot (110 peticiones cada 10 segundos)
    mediante un sistema de rate limiting integrado.
    """
    try:
        args = parse_args()
        
        # Configurar nivel de logging
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            # Mantener el nivel de logging de las bibliotecas externas en WARNING
            logging.getLogger('urllib3').setLevel(logging.WARNING)
            logging.getLogger('requests').setLevel(logging.WARNING)
            logger.debug("Modo verbose activado")
        
        logger.info("Iniciando proceso de descarga de HubSpot")
        
        # Si solo se solicita resumir emails existentes
        if args.only_summarize:
            logger.info("Modo de solo resumen activado, procesando emails existentes...")
            process_emails_in_directory(args.output_dir)
            logger.info("Proceso completado con éxito.")
            return
        
        logger.info("Nota: Este script respeta el límite de 110 peticiones cada 10 segundos de la API de HubSpot")
        
        # Obtener contactos
        if not args.skip_contacts:
            logger.info("Downloading contacts...")
            contacts = get_contacts()
            save_to_csv(contacts, 'hubspot_contacts.csv')

        # Tipos de engagements disponibles
        engagement_types = ['notes', 'emails', 'calls', 'meetings', 'tasks']

        # Descargar engagements
        if not args.skip_engagements:
            for engagement_type in engagement_types:
                logger.info(f"Downloading engagements: {engagement_type}...")
                engagements = get_engagements(engagement_type)
                save_to_csv(engagements, f'hubspot_{engagement_type}.csv')

        # Descargar contenido completo de emails
        if not args.skip_emails:
            logger.info("Starting detailed email content download...")
            download_email_contents('hubspot_emails.csv', args.output_dir, args.force, args.summarize)

        logger.info("Process completed successfully.")
    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise

if __name__ == '__main__':
    main()
