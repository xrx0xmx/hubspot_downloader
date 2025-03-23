"""
Funciones de utilidad para el procesamiento de datos de HubSpot.
"""

import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import dateutil.parser

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sanitize_filename(filename: Optional[str]) -> str:
    """Sanitiza un nombre de archivo para que sea válido en el sistema de archivos.
    
    Args:
        filename: Nombre de archivo a sanitizar
        
    Returns:
        Nombre de archivo sanitizado
    """
    # Manejar valores None
    if filename is None:
        return 'unknown'
        
    # Reemplazar caracteres no válidos con guion bajo
    filename = re.sub(r'[<>:"/\\|?*\']', '_', filename)
    # Eliminar espacios al inicio y final
    filename = filename.strip()
    # Si está vacío, usar 'unknown'
    return filename if filename else 'unknown'

def parse_date_for_filename(date_value):
    """Parsea una fecha desde diferentes formatos para usarla en un nombre de archivo.
    
    Args:
        date_value: Valor de fecha (timestamp en ms, cadena ISO, etc.)
        
    Returns:
        Cadena de fecha formateada para nombre de archivo (YYYYMMDD_HHMMSS)
    """
    try:
        if date_value is None:
            return "unknown_date"
        
        # Si es un string que parece una fecha ISO
        if isinstance(date_value, str) and ('T' in date_value or '-' in date_value):
            try:
                # Intentar parsear como fecha ISO
                dt = dateutil.parser.parse(date_value)
                return dt.strftime('%Y%m%d_%H%M%S')
            except Exception as e:
                logger.debug(f"Error parsing ISO date: {e}")
        
        # Si es un número (timestamp en ms)
        try:
            # Convertir a entero y dividir por 1000 si es necesario
            timestamp = int(float(date_value))
            # Si el timestamp es muy grande (ms), convertir a segundos
            if timestamp > 1000000000000:  # Probablemente en milisegundos
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp).strftime('%Y%m%d_%H%M%S')
        except Exception as e:
            logger.debug(f"Error parsing timestamp: {e}")
        
        # Si todo falla, devolver un valor por defecto
        return "unknown_date"
    except Exception as e:
        logger.error(f"Error formatting date {date_value}: {e}")
        return "unknown_date"

def get_email_filename(properties: Dict[str, Any], email_id: str) -> str:
    """Generar nombre de archivo para el email.
    
    Args:
        properties: Propiedades del email
        email_id: ID del email
        
    Returns:
        Nombre de archivo para el email basado en el ID de HubSpot
    """
    # Usar directamente el ID de HubSpot como nombre de archivo
    return f"{email_id}.txt"

def ensure_directory(path: str) -> Path:
    """Asegura que un directorio exista, creándolo si es necesario.
    
    Args:
        path: Ruta del directorio
        
    Returns:
        Objeto Path del directorio
    """
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path
