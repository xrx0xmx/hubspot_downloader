"""
Modelos de datos para la API de HubSpot.
"""

import pandas as pd
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

def save_to_csv(data: List[Dict], filename: str) -> None:
    """Guarda datos en un archivo CSV.
    
    Args:
        data: Datos a guardar
        filename: Nombre del archivo
    """
    if not data:
        logger.warning(f"No data to save for {filename}")
        return
    
    try:
        df = pd.json_normalize(data)
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(data)} records to {filename}")
    except Exception as e:
        logger.error(f"Error saving {filename}: {e}")
        raise
