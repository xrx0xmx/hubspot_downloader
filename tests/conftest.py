"""
Configuración para las pruebas de pytest.
"""

import os
import pytest
import logging

# Establecer la clave de API para las pruebas
os.environ['HUBSPOT_API_KEY'] = 'test_api_key_for_tests'

# Configurar logging para las pruebas
@pytest.fixture(autouse=True)
def setup_logging():
    """Configura el logging para las pruebas."""
    # Desactivar logging durante las pruebas
    logging.basicConfig(level=logging.CRITICAL)
    
    yield
    
    # Limpiar después de las pruebas si es necesario
    pass
