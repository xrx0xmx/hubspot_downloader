import unittest
import os
import tempfile
from pathlib import Path
from hubspot.utils import sanitize_filename, get_email_filename, ensure_directory

class TestUtils(unittest.TestCase):
    """Pruebas para las funciones de utilidad."""

    def test_sanitize_filename(self):
        """Prueba la función sanitize_filename."""
        # Casos de prueba
        test_cases = [
            ("normal name", "normal name"),
            ("name/with/slashes", "name_with_slashes"),
            ("name:with:colons", "name_with_colons"),
            ("name<with>brackets", "name_with_brackets"),
            ("name?with*wildcards", "name_with_wildcards"),
            ('name"with\'quotes', "name_with_quotes"),  # Corregido para usar comillas simples
            ("", "unknown"),
            (None, "unknown"),  # Manejar None correctamente
            ("   ", "unknown")
        ]
        
        for input_name, expected in test_cases:
            with self.subTest(input_name=input_name):
                # Para el caso None, verificamos que se maneje correctamente
                if input_name is None:
                    self.assertEqual(sanitize_filename(input_name), "unknown")
                else:
                    result = sanitize_filename(input_name)
                    self.assertEqual(result, expected)
    
    def test_get_email_filename(self):
        """Prueba la función get_email_filename."""
        # Caso con timestamp y asunto
        properties = {
            "hs_timestamp": "1616432400000",  # 2021-03-22 15:00:00 UTC
            "hs_email_subject": "Test Subject"
        }
        email_id = "12345"
        result = get_email_filename(properties, email_id)
        
        # Verificar que el formato es correcto, pero sin verificar la hora exacta
        # ya que puede variar según la zona horaria
        self.assertTrue(result.startswith("20210322_"))
        self.assertTrue("_Test Subject_12345.txt" in result)
        
        # Caso sin timestamp
        properties = {
            "hs_email_subject": "Test Subject"
        }
        result = get_email_filename(properties, email_id)
        self.assertEqual(result, "unknown_date_Test Subject_12345.txt")
        
        # Caso sin asunto
        properties = {
            "hs_timestamp": "1616432400000"
        }
        result = get_email_filename(properties, email_id)
        self.assertTrue(result.startswith("20210322_"))
        self.assertTrue("_no_subject_12345.txt" in result)
        
        # Caso con asunto muy largo
        properties = {
            "hs_timestamp": "1616432400000",
            "hs_email_subject": "A" * 100
        }
        result = get_email_filename(properties, email_id)
        self.assertTrue(len(result.split("_")[2]) <= 50)
    
    def test_ensure_directory(self):
        """Prueba la función ensure_directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Probar creación de directorio simple
            test_dir = os.path.join(temp_dir, "test_dir")
            dir_path = ensure_directory(test_dir)
            self.assertTrue(os.path.exists(test_dir))
            self.assertTrue(os.path.isdir(test_dir))
            self.assertEqual(dir_path, Path(test_dir))
            
            # Probar creación de directorio anidado
            nested_dir = os.path.join(temp_dir, "parent/child/grandchild")
            dir_path = ensure_directory(nested_dir)
            self.assertTrue(os.path.exists(nested_dir))
            self.assertTrue(os.path.isdir(nested_dir))
            self.assertEqual(dir_path, Path(nested_dir))
            
            # Probar que no falla si el directorio ya existe
            dir_path = ensure_directory(nested_dir)
            self.assertTrue(os.path.exists(nested_dir))
            self.assertEqual(dir_path, Path(nested_dir))

if __name__ == "__main__":
    unittest.main()
