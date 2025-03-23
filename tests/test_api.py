import unittest
import os
import math
import time
import sys
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta
from collections import deque

# Importar el módulo test_init para asegurar que la variable de entorno esté configurada
import test_init

from hubspot.api import (
    is_valid_id, 
    wait_for_rate_limit, 
    handle_rate_limit, 
    make_api_request,
    get_company,
    get_email_content
)

class TestAPI(unittest.TestCase):
    """Pruebas para las funciones de la API."""
    
    def setUp(self):
        """Configuración para las pruebas."""
        # Asegurar que la variable de entorno esté configurada
        if 'HUBSPOT_API_KEY' not in os.environ:
            os.environ['HUBSPOT_API_KEY'] = 'test_api_key'
    
    def test_is_valid_id(self):
        """Prueba la función is_valid_id."""
        # Casos válidos
        valid_ids = [123, "123", 123.0, "123.0"]
        for id_value in valid_ids:
            with self.subTest(id_value=id_value):
                self.assertTrue(is_valid_id(id_value))
        
        # Casos inválidos
        invalid_ids = [None, "", "nan", "NaN", float('nan'), " "]
        for id_value in invalid_ids:
            with self.subTest(id_value=id_value):
                self.assertFalse(is_valid_id(id_value))
    
    @patch('hubspot.api.request_timestamps', new_callable=deque)
    @patch('hubspot.api.time.sleep')
    @patch('hubspot.api.datetime')
    def test_wait_for_rate_limit(self, mock_datetime, mock_sleep, mock_timestamps):
        """Prueba la función wait_for_rate_limit."""
        # Configurar el mock de datetime
        now = datetime(2025, 3, 22, 16, 0, 0)
        mock_datetime.now.return_value = now
        
        # Caso 1: No hay timestamps previos (lista vacía)
        wait_for_rate_limit()
        # Verificar que se agregó el timestamp actual
        self.assertEqual(len(mock_timestamps), 1)
        
        # Restablecer mocks
        mock_timestamps.clear()
        mock_sleep.reset_mock()
        
        # Caso 2: Hay timestamps pero no se alcanza el límite
        for i in range(50):  # Agregar 50 timestamps (menos que el límite de 110)
            mock_timestamps.append(now - timedelta(seconds=i))
        
        wait_for_rate_limit()
        # Verificar que no se llamó a sleep
        mock_sleep.assert_not_called()
        # Verificar que ahora hay 51 timestamps
        self.assertEqual(len(mock_timestamps), 51)
        
        # Restablecer mocks
        mock_timestamps.clear()
        mock_sleep.reset_mock()
        
        # Caso 3: Se alcanza el límite y se necesita esperar
        # Llenar la cola con el máximo de timestamps (110)
        for i in range(110):
            mock_timestamps.append(now - timedelta(seconds=10))  # Todos 10 segundos atrás
            
        wait_for_rate_limit()
        # Debe esperar 0 segundos (ya pasaron 10 segundos desde el timestamp más antiguo)
        mock_sleep.assert_not_called()
        
        # Caso 4: Se alcanza el límite y el timestamp más antiguo es reciente
        mock_timestamps.clear()
        mock_sleep.reset_mock()
        
        # Llenar la cola con el máximo de timestamps (110)
        for i in range(110):
            mock_timestamps.append(now - timedelta(seconds=5))  # Todos 5 segundos atrás
            
        wait_for_rate_limit()
        # Debe esperar 5 segundos
        mock_sleep.assert_called_once_with(5.0)
    
    @patch('hubspot.api.time.sleep')
    def test_handle_rate_limit(self, mock_sleep):
        """Prueba la función handle_rate_limit."""
        # Crear una respuesta simulada con Retry-After
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {'Retry-After': '5'}
        
        handle_rate_limit(mock_response)
        mock_sleep.assert_called_once_with(5)
        
        # Restablecer mock
        mock_sleep.reset_mock()
        
        # Probar sin Retry-After (debe usar el valor predeterminado)
        mock_response.headers = {}
        handle_rate_limit(mock_response)
        mock_sleep.assert_called_once_with(10)
    
    @patch('hubspot.api.requests.get')
    @patch('hubspot.api.wait_for_rate_limit')
    def test_make_api_request(self, mock_wait, mock_get):
        """Prueba la función make_api_request."""
        # Configurar una respuesta exitosa
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'success': True}
        mock_get.return_value = mock_response
        
        # Prueba de solicitud exitosa
        result = make_api_request('https://api.test.com', {'param': 'value'})
        mock_wait.assert_called_once()
        mock_get.assert_called_once()
        self.assertEqual(result, {'success': True})
        
        # Restablecer mocks
        mock_wait.reset_mock()
        mock_get.reset_mock()
        
        # Prueba de error HTTP
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        
        with self.assertRaises(Exception):
            make_api_request('https://api.test.com', {'param': 'value'})
    
    @patch('hubspot.api.make_api_request')
    def test_get_company(self, mock_make_request):
        """Prueba la función get_company."""
        # Configurar respuesta simulada
        mock_make_request.return_value = {
            'id': '123',
            'properties': {
                'name': 'Test Company',
                'domain': 'test.com'
            }
        }
        
        # Caso 1: ID válido
        result = get_company('123')
        self.assertEqual(result['properties']['name'], 'Test Company')
        mock_make_request.assert_called_with(
            'https://api.hubapi.com/crm/v3/objects/companies/123',
            {'properties': ['name', 'domain']}
        )
        
        # Caso 2: ID con formato decimal
        mock_make_request.reset_mock()
        result = get_company('123.0')
        self.assertEqual(result['properties']['name'], 'Test Company')
        mock_make_request.assert_called_with(
            'https://api.hubapi.com/crm/v3/objects/companies/123',
            {'properties': ['name', 'domain']}
        )
        
        # Caso 3: ID inválido
        mock_make_request.reset_mock()
        result = get_company(None)
        self.assertEqual(result, {})
        mock_make_request.assert_not_called()
        
        # Caso 4: ID NaN
        mock_make_request.reset_mock()
        result = get_company(float('nan'))
        self.assertEqual(result, {})
        mock_make_request.assert_not_called()
    
    @patch('hubspot.api.make_api_request')
    def test_get_email_content(self, mock_make_request):
        """Prueba la función get_email_content."""
        # Configurar respuesta simulada
        mock_make_request.return_value = {
            'id': '456',
            'properties': {
                'hs_email_subject': 'Test Email',
                'hs_email_text': 'This is a test email'
            }
        }
        
        # Caso 1: ID válido
        result = get_email_content('456')
        self.assertEqual(result['properties']['hs_email_subject'], 'Test Email')
        mock_make_request.assert_called_with(
            'https://api.hubapi.com/crm/v3/objects/emails/456',
            {'properties': ['hs_email_subject', 'hs_email_text', 'hs_timestamp', 'hs_email_status', 
                         'hs_email_to_email', 'hs_email_from_email']}
        )
        
        # Caso 2: ID con formato decimal
        mock_make_request.reset_mock()
        result = get_email_content('456.0')
        self.assertEqual(result['properties']['hs_email_subject'], 'Test Email')
        mock_make_request.assert_called_with(
            'https://api.hubapi.com/crm/v3/objects/emails/456',
            {'properties': ['hs_email_subject', 'hs_email_text', 'hs_timestamp', 'hs_email_status', 
                         'hs_email_to_email', 'hs_email_from_email']}
        )
        
        # Caso 3: ID inválido
        mock_make_request.reset_mock()
        result = get_email_content(None)
        self.assertEqual(result, {})
        mock_make_request.assert_not_called()
        
        # Caso 4: ID NaN
        mock_make_request.reset_mock()
        result = get_email_content(float('nan'))
        self.assertEqual(result, {})
        mock_make_request.assert_not_called()

if __name__ == "__main__":
    unittest.main()
