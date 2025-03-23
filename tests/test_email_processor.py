import unittest
import os
import tempfile
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

# Importar el módulo test_init para asegurar que la variable de entorno esté configurada
import test_init

from hubspot.email_processor import save_email_content, download_email_contents

class TestEmailProcessor(unittest.TestCase):
    """Pruebas para las funciones de procesamiento de emails."""
    
    def setUp(self):
        """Configuración para las pruebas."""
        # Asegurar que la variable de entorno esté configurada
        if 'HUBSPOT_API_KEY' not in os.environ:
            os.environ['HUBSPOT_API_KEY'] = 'test_api_key'
    
    def test_save_email_content(self):
        """Prueba la función save_email_content."""
        # Crear datos de prueba
        email_data = {
            'id': '12345',
            'properties': {
                'hs_email_subject': 'Test Subject',
                'hs_email_text': 'This is a test email content.',
                'hs_timestamp': '1616432400000',  # 2021-03-22 15:00:00
                'hs_email_status': 'SENT',
                'hs_email_to_email': 'test@example.com',
                'hs_email_from_email': 'sender@example.com'
            }
        }
        
        contact_info = {
            'firstname': 'John',
            'lastname': 'Doe',
            'email': 'test@example.com'
        }
        
        company_info = {
            'properties': {
                'name': 'Test Company',
                'domain': 'testcompany.com'
            }
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Prueba de guardado exitoso
            with patch('builtins.open', mock_open()) as mock_file:
                result = save_email_content(email_data, contact_info, company_info, temp_dir)
                self.assertTrue(result)
                mock_file.assert_called_once()
                
                # Verificar que se escribió el contenido correcto
                handle = mock_file()
                self.assertTrue(handle.write.call_count >= 6)  # Al menos 6 líneas escritas
                
                # Verificar que se llamó con los argumentos correctos
                # En lugar de verificar la ruta exacta, verificamos que contenga partes clave
                file_path = mock_file.call_args[0][0]
                self.assertIn('Test Company', file_path)
                self.assertIn('John Doe', file_path)
                self.assertIn('Test Subject', file_path)
                self.assertIn('12345.txt', file_path)
            
            # Prueba con email sin contenido
            email_data_no_text = {
                'id': '12345',
                'properties': {
                    'hs_email_subject': 'Test Subject',
                    'hs_timestamp': '1616432400000'
                }
            }
            
            result = save_email_content(email_data_no_text, contact_info, company_info, temp_dir)
            self.assertFalse(result)
            
            # Prueba con error
            with patch('builtins.open', side_effect=Exception("Test error")):
                result = save_email_content(email_data, contact_info, company_info, temp_dir)
                self.assertFalse(result)
    
    @patch('hubspot.email_processor.get_email_content')
    @patch('hubspot.email_processor.get_company')
    @patch('hubspot.email_processor.save_email_content')
    @patch('hubspot.email_processor.pd.read_csv')
    @patch('hubspot.email_processor.ensure_directory')
    @patch('hubspot.email_processor.is_valid_id')
    def test_download_email_contents(self, mock_is_valid_id, mock_ensure_dir, mock_read_csv, 
                                   mock_save, mock_get_company, mock_get_email):
        """Prueba la función download_email_contents."""
        # Configurar mocks
        mock_ensure_dir.return_value = Path('/tmp/test')
        mock_is_valid_id.side_effect = lambda x: x not in [None, float('nan'), 'nan', '']
        
        # Crear DataFrames de prueba
        emails_df = pd.DataFrame({
            'id': ['1', '2', '3'],
            'properties.hs_object_id': ['101', '102', float('nan')],
            'properties.hs_email_to_email': ['contact1@example.com', 'contact2@example.com', 'contact3@example.com']
        })
        
        contacts_df = pd.DataFrame({
            'id': ['201', '202'],
            'properties.email': ['contact1@example.com', 'contact2@example.com'],
            'properties.associatedcompanyid': ['301', '302'],
            'firstname': ['John', 'Jane'],
            'lastname': ['Doe', 'Smith']
        })
        
        mock_read_csv.side_effect = [emails_df, contacts_df]
        
        # Configurar respuestas para get_company
        def mock_get_company_side_effect(company_id):
            company_data = {
                '301': {'properties': {'name': 'Company A', 'domain': 'companya.com'}},
                '302': {'properties': {'name': 'Company B', 'domain': 'companyb.com'}}
            }
            return company_data.get(company_id, {})
        
        mock_get_company.side_effect = mock_get_company_side_effect
        
        # Configurar respuestas para get_email_content
        def mock_get_email_side_effect(email_id):
            email_data = {
                '101': {
                    'id': '101',
                    'properties': {
                        'hs_email_subject': 'Email 1',
                        'hs_email_text': 'Content 1',
                        'hs_timestamp': '1616432400000'
                    }
                },
                '102': {
                    'id': '102',
                    'properties': {
                        'hs_email_subject': 'Email 2',
                        'hs_email_text': 'Content 2',
                        'hs_timestamp': '1616518800000'
                    }
                }
            }
            return email_data.get(email_id, {})
        
        mock_get_email.side_effect = mock_get_email_side_effect
        
        # Configurar save_email_content para devolver True
        mock_save.return_value = True
        
        # Ejecutar la función
        download_email_contents('test_emails.csv', '/tmp/output', False)
        
        # Verificaciones
        mock_ensure_dir.assert_called_once_with('/tmp/output')
        self.assertEqual(mock_read_csv.call_count, 2)
        
        # Debe haber llamadas a get_company para cada contacto válido con compañía
        self.assertTrue(mock_get_company.call_count >= 2)
        
        # Debe haber 3 llamadas a get_email_content (una por cada email en el DataFrame)
        self.assertEqual(mock_get_email.call_count, 3)
        
        # Debe haber 2 llamadas a save_email_content (una por cada email válido)
        self.assertEqual(mock_save.call_count, 2)

if __name__ == "__main__":
    unittest.main()
