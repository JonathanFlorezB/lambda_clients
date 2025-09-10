import unittest
from unittest.mock import MagicMock, Mock
import math
from typing import Dict, List, Tuple, Optional
import logging
from app.db_utils import (
    get_paginated_data,
    validate_data,
    validate_actions,
    count_codigo_occurrences,
    validate_items,
    validate_and_process_client_data,
    get_client_info,
)

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestDbUtils(unittest.TestCase):
    def setUp(self):
        # Configuración del mock del cursor
        self.cursor = MagicMock()
        # Configuración de ejemplo para resource_config
        self.resource_config = {
            "db_schema": "public",
            "db_table": "clients",
            "limit": 10,
            "columns": ["id_cliente", "codigo_identificacion", "tipo_identificacion", "fecha_afiliacion"],
            "search_fields": ["nombre", "apellido"],
            "required_fields": ["codigo_identificacion", "tipo_identificacion", "fecha_afiliacion"],
            "valid_types": ["numeric", "varchar"],
        }
        self.contactabilidad_config = {
            "db_schema": "public",
            "db_table": "contactabilidad",
        }
        self.productos_config = {
            "db_schema": "public",
            "db_table": "productos",
        }

    def test_get_paginated_data_no_records(self):
        # Simular cero registros
        self.cursor.fetchone.return_value = (0,)
        self.cursor.fetchall.return_value = []
        self.cursor.description = [("id_cliente",), ("codigo_identificacion",)]
        query_params = {"page": "1"}
        result = get_paginated_data(self.cursor, self.resource_config, query_params)
        self.assertEqual(result["pagination"]["totalRecords"], 0)
        self.assertEqual(result["pagination"]["totalPages"], 1)
        self.assertEqual(result["data"], [])

    def test_get_paginated_data_with_filters(self):
        # Simular filtros avanzados
        self.cursor.description = [("id_cliente",), ("nombre",)]
        self.cursor.fetchone.return_value = (50,)
        self.cursor.fetchall.return_value = [("uuid1", "Juan"), ("uuid2", "Maria")]
        query_params = {"page": "2", "nombre": "Juan", "id_cliente": "uuid1"}
        result = get_paginated_data(self.cursor, self.resource_config, query_params)
        self.assertEqual(result["pagination"]["currentPage"], 2)
        self.assertEqual(len(result["data"]), 2)
        self.cursor.execute.assert_called()

    def test_validate_and_process_client_data_error(self):
        data = [
            {"accion": "M", "codigo_identificacion": "COD001"},
        ]
        self.cursor.fetchone.return_value = (0,)  # No existe en la base de datos
        self.cursor.rowcount = 0  # Simula fallo en actualización
        result = validate_and_process_client_data(self.cursor, self.resource_config, data)
        self.assertEqual(result["updated_count"], 0)
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("codigo_identificacion", result["errors"][0]["errors"])

    def test_handle_insert_exception(self):
        data = [
            {"accion": "A", "codigo_identificacion": "COD001", "tipo_identificacion": "01", "fecha_afiliacion": "2023-10-01"},
        ]
        self.cursor.execute.side_effect = Exception("Database error")
        result = validate_and_process_client_data(self.cursor, self.resource_config, data)
        self.assertEqual(result["inserted_count"], 0)
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("general", result["errors"][0]["errors"])

    def test_handle_update_no_rows_affected(self):
        data = [
            {"accion": "M", "codigo_identificacion": "COD001", "tipo_identificacion": "01"},
        ]
        self.cursor.fetchone.return_value = (1,)  # Existe en la base de datos
        self.cursor.rowcount = 0  # Simula que no se actualizó
        result = validate_and_process_client_data(self.cursor, self.resource_config, data)
        self.assertEqual(result["updated_count"], 0)
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("general", result["errors"][0]["errors"])

    def test_handle_delete_no_rows_affected(self):
        data = [
            {"accion": "E", "codigo_identificacion": "COD001"},
        ]
        self.cursor.fetchone.return_value = (1,)  # Existe en la base de datos
        self.cursor.rowcount = 0  # Simula que no se eliminó
        result = validate_and_process_client_data(self.cursor, self.resource_config, data)
        self.assertEqual(result["deleted_count"], 0)
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("general", result["errors"][0]["errors"])

    def test_get_client_info_no_data(self):
        id_cliente = "uuid1"
        self.cursor.fetchone.return_value = None
        self.cursor.fetchall.return_value = []
        result = get_client_info(self.cursor, self.contactabilidad_config, self.productos_config, id_cliente)
        self.assertIsNone(result["contactabilidad"])
        self.assertEqual(result["productos"], [])

    def test_get_client_info_db_error(self):
        id_cliente = "uuid1"
        self.cursor.execute.side_effect = Exception("Database error")
        with self.assertRaises(Exception):
            get_client_info(self.cursor, self.contactabilidad_config, self.productos_config, id_cliente)

if __name__ == "__main__":
    unittest.main()
