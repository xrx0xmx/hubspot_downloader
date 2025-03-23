"""
Inicialización para pruebas.
Este archivo se ejecuta antes de cualquier prueba para configurar el entorno.
"""

import os
import sys

# Establecer la clave de API para las pruebas
os.environ['HUBSPOT_API_KEY'] = 'test_api_key_for_tests'

# Añadir el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
