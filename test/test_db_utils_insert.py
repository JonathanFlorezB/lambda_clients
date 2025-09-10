import pytest
from unittest.mock import MagicMock, patch
from app.db_utils import (
    get_paginated_data,
    validate_data,
    validate_actions,
    count_codigo_occurrences,
    validate_items,
    validate_item_fields,
    validate_tipo_identificacion,
    validate_fecha_afiliacion,
    validate_database_operations,
    validate_and_process_client_data,
    validate_client_data,
    validate_client_item,
    check_existence_in_db,
    process_valid_client_data,
    process_client_item,
    handle_insert,
    handle_update,
    handle_delete,
    get_client_info
)

# Enhanced MockCursor with connection attribute
class MockCursor:
    def __init__(self):
        self.execute = MagicMock()
        self.fetchone = MagicMock(return_value=[0])
        self.fetchall = MagicMock(return_value=[('data1', 'data2')])
        self.close = MagicMock()
        self.description = [('column1',), ('column2',)]
        self.rowcount = 1
        self.connection = MagicMock()  # Added connection attribute

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

# Fixtures
@pytest.fixture
def mock_cursor():
    return MockCursor()

@pytest.fixture
def client_resource_config():
    return {
        "db_schema": "schema",
        "db_table": "clientes",
        "limit": 10,
        "search_fields": ["nombre", "codigo_identificacion"],
        "required_fields": ["codigo_identificacion", "tipo_identificacion", "nombre"],
        "columns": ["id_cliente", "codigo_identificacion", "tipo_identificacion", "nombre", "fecha_afiliacion"]
    }

@pytest.fixture
def valid_client_data():
    return [{
        "accion": "A",
        "codigo_identificacion": "1234567890",
        "tipo_identificacion": "01",
        "nombre": "Juan Perez",
        "fecha_afiliacion": "2023-01-01"
    }]

# Tests for get_paginated_data
def test_get_paginated_data_basic(mock_cursor, client_resource_config):
    query_params = {}
    mock_cursor.fetchone.return_value = [5]  # total_records
    
    result = get_paginated_data(mock_cursor, client_resource_config, query_params)
    
    assert "pagination" in result
    assert "data" in result
    assert result["pagination"]["currentPage"] == 1
    assert result["pagination"]["totalPages"] == 1

def test_get_paginated_data_with_search(mock_cursor, client_resource_config):
    query_params = {"nombre": "juan"}
    mock_cursor.fetchone.return_value = [3]  # total_records
    
    result = get_paginated_data(mock_cursor, client_resource_config, query_params)
    
    assert result["pagination"]["totalRecords"] == 3

def test_get_paginated_data_with_id_cliente(mock_cursor, client_resource_config):
    query_params = {"id_cliente": "uuid-test"}
    mock_cursor.fetchone.return_value = [1]  # total_records
    
    result = get_paginated_data(mock_cursor, client_resource_config, query_params)
    
    assert result["pagination"]["totalRecords"] == 1

# Tests for validate_actions
def test_validate_actions_valid():
    data = [{"accion": "A"}, {"accion": "M"}, {"accion": "E"}]
    errors = validate_actions(data)
    assert len(errors) == 0

def test_validate_actions_invalid():
    data = [{"accion": "X"}, {"accion": "Y"}]
    errors = validate_actions(data)
    assert len(errors) == 2

def test_validate_actions_missing():
    data = [{"nombre": "test"}]
    errors = validate_actions(data)
    assert len(errors) == 1

# Tests for count_codigo_occurrences
def test_count_codigo_occurrences():
    data = [
        {"accion": "A", "codigo_identificacion": "123"},
        {"accion": "M", "codigo_identificacion": "123"},
        {"accion": "E", "codigo_identificacion": "456"}
    ]
    counts = count_codigo_occurrences(data)
    assert counts["123"] == 2
    assert counts["456"] == 1

# Tests for validate_item_fields
def test_validate_item_fields_add_valid(client_resource_config):
    item = {"accion": "A", "codigo_identificacion": "123", "tipo_identificacion": "01", "nombre": "test"}
    errors = validate_item_fields(item, "A", client_resource_config)
    assert len(errors) == 0

def test_validate_item_fields_add_invalid(client_resource_config):
    item = {"accion": "A", "codigo_identificacion": ""}
    errors = validate_item_fields(item, "A", client_resource_config)
    assert len(errors) > 0

def test_validate_item_fields_modify_valid(client_resource_config):
    item = {"accion": "M", "codigo_identificacion": "123"}
    errors = validate_item_fields(item, "M", client_resource_config)
    assert len(errors) == 0

# Tests for validate_tipo_identificacion
def test_validate_tipo_identificacion_valid():
    item = {"tipo_identificacion": "01"}
    errors = validate_tipo_identificacion(item, "A")
    assert len(errors) == 0

def test_validate_tipo_identificacion_invalid():
    item = {"tipo_identificacion": "99"}
    errors = validate_tipo_identificacion(item, "A")
    assert len(errors) == 1

def test_validate_tipo_identificacion_wrong_format():
    item = {"tipo_identificacion": "1"}
    errors = validate_tipo_identificacion(item, "A")
    assert len(errors) == 1

# Tests for validate_fecha_afiliacion
def test_validate_fecha_afiliacion_valid():
    item = {"fecha_afiliacion": "2023-01-01"}
    errors = validate_fecha_afiliacion(item, "A")
    assert len(errors) == 0

def test_validate_fecha_afiliacion_invalid():
    item = {"fecha_afiliacion": "2023/01/01"}
    errors = validate_fecha_afiliacion(item, "A")
    assert len(errors) == 1

# Tests for validate_database_operations
def test_validate_database_operations_insert_new(mock_cursor):
    item = {"accion": "A", "codigo_identificacion": "123"}
    mock_cursor.fetchone.return_value = [0]  # Doesn't exist
    errors = validate_database_operations(mock_cursor, item, "A", "test_table", {})
    assert len(errors) == 0

def test_validate_database_operations_insert_duplicate(mock_cursor):
    item = {"accion": "A", "codigo_identificacion": "123"}
    mock_cursor.fetchone.return_value = [1]  # Already exists
    errors = validate_database_operations(mock_cursor, item, "A", "test_table", {})
    assert len(errors) == 1

def test_validate_database_operations_update_exists(mock_cursor):
    item = {"accion": "M", "codigo_identificacion": "123"}
    mock_cursor.fetchone.return_value = [1]  # Exists
    errors = validate_database_operations(mock_cursor, item, "M", "test_table", {})
    assert len(errors) == 0

def test_validate_database_operations_update_not_exists(mock_cursor):
    item = {"accion": "M", "codigo_identificacion": "123"}
    mock_cursor.fetchone.return_value = [0]  # Doesn't exist
    errors = validate_database_operations(mock_cursor, item, "M", "test_table", {})
    assert len(errors) == 1

# Tests for validate_client_item
def test_validate_client_item_valid(mock_cursor, client_resource_config):
    table_name = "test_table"
    item = {"accion": "A", "codigo_identificacion": "123", "tipo_identificacion": "01", "nombre": "test"}
    mock_cursor.fetchone.return_value = [0]  # Doesn't exist for insert
    
    errors = validate_client_item(mock_cursor, client_resource_config, table_name, item, "A")
    assert len(errors) == 0

def test_validate_client_item_missing_required(mock_cursor, client_resource_config):
    table_name = "test_table"
    item = {"accion": "A", "codigo_identificacion": "123"}
    
    errors = validate_client_item(mock_cursor, client_resource_config, table_name, item, "A")
    assert len(errors) > 0

# Tests for check_existence_in_db
def test_check_existence_in_db_exists(mock_cursor):
    table_name = "test_table"
    codigo = "123"
    field_errors = {}
    mock_cursor.fetchone.return_value = [1]  # Exists
    
    check_existence_in_db(mock_cursor, table_name, codigo, field_errors, "M")
    assert len(field_errors) == 0

def test_check_existence_in_db_not_exists(mock_cursor):
    table_name = "test_table"
    codigo = "123"
    field_errors = {}
    mock_cursor.fetchone.return_value = [0]  # Doesn't exist
    
    check_existence_in_db(mock_cursor, table_name, codigo, field_errors, "M")
    assert len(field_errors) == 1

# Tests for handle_insert
def test_handle_insert_success(mock_cursor, client_resource_config):
    table_name = "test_table"
    item = {
        "codigo_identificacion": "123",
        "tipo_identificacion": "01",
        "nombre": "test"
    }
    
    result = handle_insert(mock_cursor, client_resource_config, table_name, item)
    assert result["success"] == True
    assert "id_cliente" in item  # Should be added by handle_insert

# Tests for handle_update
def test_handle_update_success(mock_cursor, client_resource_config):
    table_name = "test_table"
    item = {
        "codigo_identificacion": "123",
        "nombre": "updated name"
    }
    mock_cursor.rowcount = 1
    
    result = handle_update(mock_cursor, client_resource_config, table_name, item, 0)
    assert result["success"] == True

def test_handle_update_failure(mock_cursor, client_resource_config):
    table_name = "test_table"
    item = {
        "codigo_identificacion": "123",
        "nombre": "updated name"
    }
    mock_cursor.rowcount = 0
    
    result = handle_update(mock_cursor, client_resource_config, table_name, item, 0)
    assert result["success"] == False

# Tests for handle_delete
def test_handle_delete_success(mock_cursor):
    table_name = "test_table"
    item = {"codigo_identificacion": "123"}
    mock_cursor.rowcount = 1
    
    result = handle_delete(mock_cursor, table_name, item, 0)
    assert result["success"] == True

def test_handle_delete_failure(mock_cursor):
    table_name = "test_table"
    item = {"codigo_identificacion": "123"}
    mock_cursor.rowcount = 0
    
    result = handle_delete(mock_cursor, table_name, item, 0)
    assert result["success"] == False

# Tests for get_client_info
def test_get_client_info_success(mock_cursor):
    contactabilidad_config = {
        "db_schema": "schema",
        "db_table": "contactabilidad"
    }
    productos_config = {
        "db_schema": "schema",
        "db_table": "productos"
    }
    id_cliente = "test-uuid"
    
    # Mock contactabilidad response
    mock_cursor.fetchone.return_value = ("contact_data",)
    mock_cursor.description = [("column",)]
    
    # Mock productos response
    mock_cursor.fetchall.return_value = [("product_data",)]
    
    result = get_client_info(mock_cursor, contactabilidad_config, productos_config, id_cliente)
    
    assert "contactabilidad" in result
    assert "productos" in result
