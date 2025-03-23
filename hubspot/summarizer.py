"""
Módulo para resumir emails usando OpenAI.
"""

import logging
import os
import json
from pathlib import Path
from typing import Dict, Optional, Any

from openai import OpenAI

logger = logging.getLogger(__name__)

# Prompt para resumir emails
EMAIL_SUMMARY_PROMPT = """
Eres un asistente especializado en resumir emails de manera detallada y profesional.
A continuación se presenta un email. Por favor, genera un resumen detallado que incluya:

1. Asunto principal del email
2. Puntos clave discutidos
3. Cualquier acción requerida o fecha límite mencionada
4. Tono general del email (formal, informal, urgente, etc.)
5. Cualquier información importante como números, fechas o datos relevantes

Email:
{email_content}

Proporciona un resumen detallado y estructurado.
"""

def get_openai_client() -> Optional[OpenAI]:
    """
    Crea y devuelve un cliente de OpenAI.
    
    Returns:
        Cliente de OpenAI o None si no se puede crear
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error("No se encontró la variable de entorno OPENAI_API_KEY")
        return None
    
    try:
        return OpenAI(api_key=api_key)
    except Exception as e:
        logger.error(f"Error al crear el cliente de OpenAI: {e}")
        return None

def summarize_email(email_content: str) -> Optional[Dict[str, Any]]:
    """
    Genera un resumen detallado de un email usando OpenAI.
    
    Args:
        email_content: Contenido del email a resumir
        
    Returns:
        Diccionario con el resumen del email o None si hay un error
    """
    client = get_openai_client()
    if not client:
        return None
    
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Eres un asistente especializado en resumir emails."},
                {"role": "user", "content": EMAIL_SUMMARY_PROMPT.format(email_content=email_content)}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        summary = response.choices[0].message.content
        
        return {
            "summary": summary,
            "model": "gpt-3.5-turbo",
            "timestamp": os.path.getmtime(Path(__file__))
        }
    except Exception as e:
        logger.error(f"Error al generar el resumen del email: {e}")
        return None

def process_email_file(email_path: Path) -> bool:
    """
    Procesa un archivo de email para generar y guardar su resumen.
    
    Args:
        email_path: Ruta al archivo de email
        
    Returns:
        True si se procesó correctamente, False en caso contrario
    """
    try:
        # Verificar si ya existe un resumen
        summary_path = email_path.with_suffix('.summary.json')
        if summary_path.exists():
            logger.debug(f"El resumen ya existe para {email_path.name}, omitiendo...")
            return False
        
        # Leer el contenido del email
        with open(email_path, 'r', encoding='utf-8') as f:
            email_content = f.read()
        
        # Generar el resumen
        summary_data = summarize_email(email_content)
        if not summary_data:
            logger.error(f"No se pudo generar el resumen para {email_path.name}")
            return False
        
        # Guardar el resumen
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, ensure_ascii=False, indent=2)
        
        logger.debug(f"Resumen guardado para {email_path.name}")
        return True
    
    except Exception as e:
        logger.error(f"Error al procesar el email {email_path.name}: {e}")
        return False

def process_emails_in_directory(directory: str) -> Dict[str, int]:
    """
    Procesa todos los emails en un directorio y sus subdirectorios.
    
    Args:
        directory: Directorio base donde buscar emails
        
    Returns:
        Diccionario con estadísticas del procesamiento
    """
    stats = {
        "total": 0,
        "processed": 0,
        "skipped": 0,
        "errors": 0
    }
    
    try:
        base_dir = Path(directory)
        if not base_dir.exists() or not base_dir.is_dir():
            logger.error(f"El directorio {directory} no existe o no es un directorio")
            return stats
        
        # Buscar todos los archivos .txt (emails)
        email_files = list(base_dir.glob('**/*.txt'))
        stats["total"] = len(email_files)
        
        logger.info(f"Procesando {stats['total']} emails para resumir...")
        
        for i, email_path in enumerate(email_files):
            if i % 10 == 0:
                logger.info(f"Procesando email {i+1} de {stats['total']}...")
            
            if process_email_file(email_path):
                stats["processed"] += 1
            else:
                # Verificar si ya existe un resumen
                if email_path.with_suffix('.summary.json').exists():
                    stats["skipped"] += 1
                else:
                    stats["errors"] += 1
        
        logger.info(f"Procesamiento de resúmenes completado:")
        logger.info(f"- Total emails: {stats['total']}")
        logger.info(f"- Emails procesados: {stats['processed']}")
        logger.info(f"- Emails omitidos (ya resumidos): {stats['skipped']}")
        logger.info(f"- Emails con errores: {stats['errors']}")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error al procesar los emails: {e}")
        stats["errors"] = stats["total"]
        return stats
