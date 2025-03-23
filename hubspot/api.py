"""
Módulo para interactuar con la API de HubSpot.
"""

import os
import time
import logging
import requests
import math
from typing import Dict, List, Any, Optional
from threading import Lock
from collections import deque
from datetime import datetime, timedelta
import sys
import json
from pathlib import Path

logger = logging.getLogger(__name__)

# Configuración de la API
API_KEY = os.getenv('HUBSPOT_API_KEY')
if not API_KEY and 'pytest' not in sys.modules and __name__ != '__main__':
    raise ValueError("HUBSPOT_API_KEY environment variable is not set")
elif not API_KEY:
    # Para pruebas o ejecución directa del módulo, usar una clave ficticia
    API_KEY = 'test_api_key'
    logger.warning("Using test API key. This will not work for real API calls.")

BASE_URL = 'https://api.hubapi.com'
headers = {"Authorization": f"Bearer {API_KEY}"}

# Rate limiting configuration
RATE_LIMIT = 110  # requests
RATE_WINDOW = 10  # seconds
request_timestamps = deque(maxlen=RATE_LIMIT)
rate_limit_lock = Lock()

def wait_for_rate_limit():
    """Espera si es necesario para respetar el límite de tasa de la API.
    
    HubSpot permite 110 peticiones cada 10 segundos.
    """
    with rate_limit_lock:
        now = datetime.now()
        
        # Eliminar timestamps antiguos (más de 10 segundos)
        while request_timestamps and (now - request_timestamps[0]) > timedelta(seconds=RATE_WINDOW):
            request_timestamps.popleft()
        
        # Si estamos en el límite, esperar hasta que podamos hacer otra petición
        if len(request_timestamps) >= RATE_LIMIT:
            oldest = request_timestamps[0]
            wait_time = (oldest + timedelta(seconds=RATE_WINDOW) - now).total_seconds()
            if wait_time > 0:
                logger.debug(f"Rate limit approaching, waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
        
        # Registrar esta petición
        request_timestamps.append(now)

def handle_rate_limit(response: requests.Response) -> None:
    """Maneja el rate limiting esperando si es necesario.
    
    Args:
        response: Respuesta de la API que contiene información de rate limiting
    """
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 10))
        logger.warning(f"Rate limit hit. Waiting {retry_after} seconds...")
        time.sleep(retry_after)

def make_api_request(url: str, params: Dict[str, Any]) -> Dict:
    """Realiza una solicitud a la API con manejo de rate limiting.
    
    Args:
        url: URL de la API
        params: Parámetros de la solicitud
        
    Returns:
        Datos de la respuesta en formato JSON
        
    Raises:
        requests.exceptions.RequestException: Si la solicitud falla después de los reintentos
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Esperar si es necesario para respetar el límite de tasa
            wait_for_rate_limit()
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                handle_rate_limit(response)
                continue
            logger.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
    return {}

def get_contacts() -> List[Dict]:
    """Obtiene todos los contactos de HubSpot con información de la empresa.
    
    Returns:
        Lista de contactos
    """
    contacts = []
    after = None
    while True:
        params = {
            'limit': 100,
            'properties': ['email', 'firstname', 'lastname', 'company', 'hs_object_id', 'associatedcompanyid']
        }
        if after:
            params['after'] = after
        
        data = make_api_request(f'{BASE_URL}/crm/v3/objects/contacts', params)
        contacts.extend(data.get('results', []))
        
        if 'paging' in data and 'next' in data['paging']:
            after = data['paging']['next']['after']
        else:
            break
    return contacts

def is_valid_id(id_value: Any) -> bool:
    """Verifica si un ID es válido (no es NaN, None, etc.).
    
    Args:
        id_value: Valor del ID a verificar
        
    Returns:
        True si el ID es válido, False en caso contrario
    """
    if id_value is None:
        return False
    
    # Convertir a string para verificar si es 'nan' o vacío
    if str(id_value).lower() == 'nan' or str(id_value).strip() == '':
        return False
    
    # Verificar si es un número float NaN
    if isinstance(id_value, float) and math.isnan(id_value):
        return False
    
    return True

def get_company(company_id: Any) -> Dict:
    """Obtiene información de una empresa específica.
    
    Args:
        company_id: ID de la empresa
        
    Returns:
        Información de la empresa
    """
    # Verificar si el ID es válido
    if not is_valid_id(company_id):
        logger.debug(f"Skipping invalid company ID: {company_id}")
        return {}
    
    try:
        # Convertir el ID a entero eliminando cualquier parte decimal
        company_id_int = str(int(float(company_id)))
        
        # Primero intentar cargar desde disco
        cached_company = load_company_from_disk(company_id_int)
        if cached_company:
            company_name = cached_company.get('properties', {}).get('name', 'Desconocida')
            logger.info(f"📂 Empresa cargada desde caché: ID={company_id_int}, Nombre='{company_name}'")
            return cached_company
        
        # Si no está en disco, obtener de la API
        logger.info(f"🔄 Obteniendo empresa {company_id_int} desde la API de HubSpot...")
        data = make_api_request(f'{BASE_URL}/crm/v3/objects/companies/{company_id_int}', {
            'properties': ['name', 'domain']
        })
        
        if data:
            company_name = data.get('properties', {}).get('name', 'Desconocida')
            logger.info(f"✅ Empresa obtenida de API: ID={company_id_int}, Nombre='{company_name}'")
            
            # Guardar en disco para futuras consultas
            save_company_to_disk(company_id_int, data)
            logger.info(f"💾 Empresa {company_id_int} guardada en disco")
            
        return data
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid company ID format: {company_id} - {e}")
        return {}
    except Exception as e:
        logger.error(f"Error getting company info for ID {company_id}: {e}")
        return {}

def get_engagements(engagement_type: str) -> List[Dict]:
    """Obtiene todos los engagements de un tipo específico.
    
    Args:
        engagement_type: Tipo de engagement (emails, calls, meetings, etc.)
        
    Returns:
        Lista de engagements
    """
    engagements = []
    after = None
    while True:
        params = {'limit': 100}
        if after:
            params['after'] = after
        
        data = make_api_request(f'{BASE_URL}/crm/v3/objects/{engagement_type}', params)
        engagements.extend(data.get('results', []))
        
        if 'paging' in data and 'next' in data['paging']:
            after = data['paging']['next']['after']
        else:
            break
    return engagements

def get_email_content(email_id: Any) -> Dict:
    """Obtiene el contenido detallado de un email específico.
    
    Args:
        email_id: ID del email
        
    Returns:
        Contenido detallado del email
    """
    # Verificar si el ID es válido
    if not is_valid_id(email_id):
        logger.debug(f"Skipping invalid email ID: {email_id}")
        return {}
    
    try:
        # Convertir el ID a entero eliminando cualquier parte decimal
        email_id_int = str(int(float(email_id)))
        
        logger.info(f"Obteniendo email con ID {email_id_int} de la API de HubSpot...")
        data = make_api_request(f'{BASE_URL}/crm/v3/objects/emails/{email_id_int}', {
            'properties': ['hs_email_subject', 'hs_email_text', 'hs_timestamp', 'hs_email_status', 
                         'hs_email_to_email', 'hs_email_from_email']
        })
        
        if data:
            subject = data.get('properties', {}).get('hs_email_subject', 'Sin asunto')
            logger.info(f"Email obtenido correctamente: ID={email_id_int}, Asunto='{subject}'")
        else:
            logger.warning(f"No se pudo obtener datos para el email con ID {email_id_int}")
            
        return data
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid email ID format: {email_id} - {e}")
        return {}
    except Exception as e:
        logger.error(f"Error getting email content for ID {email_id}: {e}")
        return {}

# Directorio para guardar la información de empresas
COMPANIES_CACHE_DIR = 'companies_cache'

def ensure_directory(directory_path: str) -> str:
    """Asegura que un directorio exista, creándolo si es necesario.
    
    Args:
        directory_path: Ruta del directorio
        
    Returns:
        Ruta del directorio creado
    """
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    return str(path)

def get_company_cache_path(company_id: str) -> str:
    """Obtiene la ruta del archivo de caché para una empresa.
    
    Args:
        company_id: ID de la empresa
        
    Returns:
        Ruta del archivo de caché
    """
    ensure_directory(COMPANIES_CACHE_DIR)
    return os.path.join(COMPANIES_CACHE_DIR, f"company_{company_id}.json")

def save_company_to_disk(company_id: str, company_data: Dict) -> None:
    """Guarda la información de una empresa en disco.
    
    Args:
        company_id: ID de la empresa
        company_data: Datos de la empresa
    """
    try:
        cache_path = get_company_cache_path(company_id)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(company_data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        logger.debug(f"Información de empresa {company_id} guardada en {cache_path}")
    except Exception as e:
        logger.error(f"Error al guardar información de empresa {company_id}: {e}")

def load_company_from_disk(company_id: str) -> Optional[Dict]:
    """Carga la información de una empresa desde disco.
    
    Args:
        company_id: ID de la empresa
        
    Returns:
        Datos de la empresa o None si no existe
    """
    try:
        cache_path = get_company_cache_path(company_id)
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                company_data = json.load(f)
            logger.debug(f"Información de empresa {company_id} cargada desde {cache_path}")
            return company_data
        return None
    except Exception as e:
        logger.error(f"Error al cargar información de empresa {company_id}: {e}")
        return None
