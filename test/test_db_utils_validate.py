import pytest
from unittest.mock import MagicMock, patch
from app.db_utils import (
    validate_actions,
    count_codigo_occurrences,
    validate_items,
    validate_item_fields,
    validate_tipo_identificacion,
    validate_fecha_afiliacion,
    validate_database_operations
)

# Fixtures
@pytest.fixture
def mock_cursor():
    cursor = MagicMock()
    cursor.fetchone.return_value = [0]  # Default to not found
    return cursor

@pytest.fixture
def clientes_resource_config():
    return {
        "db_schema": "public",
        "db_table": "clientes",
        "required_fields": ["codigo_identificacion", "tipo_identificacion", "fecha_afiliacion"],
        "search_fields": ["nombre"],
        "columns": ["id_cliente", "codigo_identificacion", "tipo_identificacion", "fecha_afiliacion"]
    }

# Tests for validate_actions
def test_validate_actions_valid():
    data = [
        {"accion": "A"},
        {"accion": "M"},
        {"accion": "E"}
    ]
    errors = validate_actions(data)
    assert len(errors) == 0

def test_validate_actions_invalid():
    data = [
        {"accion": "X"},  # Invalid action
        {},               # Missing action
        {"accion": "A"}   # Valid action
    ]
    errors = validate_actions(data)
    assert len(errors) == 2
    assert all("accion" in error["errors"] for error in errors)

# Tests for count_codigo_occurrences
def test_count_codigo_occurrences():
    data = [
        {"accion": "A", "codigo_identificacion": "123"},
        {"accion": "M", "codigo_identificacion": "123"},
        {"accion": "E", "codigo_identificacion": "456"},
        {"accion": "X"},  # Invalid action, should be ignored
        {}                # No action, should be ignored
    ]
    counts = count_codigo_occurrences(data)
    assert counts == {"123": 2, "456": 1}

# Tests for validate_items
def test_validate_items_success(mock_cursor, clientes_resource_config):
    data = [{
        "accion": "A",
        "codigo_identificacion": "123",
        "tipo_identificacion": "01",
        "fecha_afiliacion": "2023-01-01"
    }]
    table_name = "mstx.clientes"
    codigo_counts = {"123": 1}
    existing_errors = []

    errors = validate_items(mock_cursor, data, clientes_resource_config,
                            table_name, codigo_counts, existing_errors)
    assert len(errors) == 0

# Tests for validate_item_fields
def test_validate_item_fields_insert(clientes_resource_config):
    item = {
        "accion": "A",
        "codigo_identificacion": "123",
        "tipo_identificacion": "01",
        "fecha_afiliacion": "2023-01-01"
    }
    errors = validate_item_fields(item, "A", clientes_resource_config)
    assert len(errors) == 0

def test_validate_item_fields_update(clientes_resource_config):
    item = {
        "accion": "M",
        "codigo_identificacion": "123"  # Only codigo_identificacion required for M/E
    }
    errors = validate_item_fields(item, "M", clientes_resource_config)
    assert len(errors) == 0

def test_validate_item_fields_missing(clientes_resource_config):
    item = {"accion": "A"}  # Missing all required fields
    errors = validate_item_fields(item, "A", clientes_resource_config)
    assert len(errors) == 3  # All required fields should have errors

# Tests for validate_tipo_identificacion
def test_validate_tipo_identificacion_valid():
    item = {"tipo_identificacion": "05"}
    errors = validate_tipo_identificacion(item, "A")
    assert len(errors) == 0

def test_validate_tipo_identificacion_invalid():
    item = {"tipo_identificacion": "99"}  # Invalid value
    errors = validate_tipo_identificacion(item, "A")
    assert "tipo_identificacion" in errors

def test_validate_tipo_identificacion_not_required_for_delete():
    item = {}  # No tipo_identificacion
    errors = validate_tipo_identificacion(item, "E")  # Delete action
    assert len(errors) == 0

# Tests for validate_fecha_afiliacion
def test_validate_fecha_afiliacion_valid():
    item = {"fecha_afiliacion": "2023-01-01"}
    errors = validate_fecha_afiliacion(item, "A")
    assert len(errors) == 0

def test_validate_fecha_afiliacion_invalid_format():
    item = {"fecha_afiliacion": "01/01/2023"}  # Wrong format
    errors = validate_fecha_afiliacion(item, "A")
    assert "fecha_afiliacion" in errors

def test_validate_fecha_afiliacion_not_required_for_delete():
    item = {}  # No fecha_afiliacion
    errors = validate_fecha_afiliacion(item, "E")  # Delete action
    assert len(errors) == 0

# Tests for validate_database_operations
def test_validate_database_operations_insert_new(mock_cursor):
    item = {"accion": "A", "codigo_identificacion": "123"}
    table_name = "clientes"
    codigo_counts = {"123": 1}
    mock_cursor.fetchone.return_value = [0]  # Not found in DB

    errors = validate_database_operations(
        mock_cursor, item, "A", table_name, codigo_counts)
    assert len(errors) == 0

def test_validate_database_operations_insert_duplicate(mock_cursor):
    item = {"accion": "A", "codigo_identificacion": "123"}
    table_name = "clientes"
    codigo_counts = {"123": 2}  # Duplicate in request

    errors = validate_database_operations(
        mock_cursor, item, "A", table_name, codigo_counts)
    assert "codigo_identificacion" in errors

def test_validate_database_operations_update_not_found(mock_cursor):
    item = {"accion": "M", "codigo_identificacion": "123"}
    table_name = "clientes"
    codigo_counts = {}
    mock_cursor.fetchone.return_value = [0]  # Not found in DB

    errors = validate_database_operations(
        mock_cursor, item, "M", table_name, codigo_counts)
    assert "codigo_identificacion" in errors

def test_validate_database_operations_insert_exists_in_db(mock_cursor):
    item = {"accion": "A", "codigo_identificacion": "123"}
    table_name = "clientes"
    codigo_counts = {"123": 1}
    mock_cursor.fetchone.return_value = [1]  # Exists in DB

    errors = validate_database_operations(
        mock_cursor, item, "A", table_name, codigo_counts)
    assert "codigo_identificacion" in errors
    assert "ya existe" in errors["codigo_identificacion"]
