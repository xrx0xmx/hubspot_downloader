#!/usr/bin/env python
"""
Script para ejecutar todas las pruebas del proyecto.
"""

import unittest
import sys
import os

def run_tests():
    """Ejecuta todas las pruebas del proyecto."""
    # Establecer la variable de entorno para las pruebas
    os.environ['HUBSPOT_API_KEY'] = 'test_api_key_for_tests'
    
    # Asegurarse de que el directorio raíz está en el path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    # Descubrir todas las pruebas
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
    
    # Ejecutar las pruebas
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    # Devolver código de salida basado en el resultado
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    sys.exit(run_tests())
