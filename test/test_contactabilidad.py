import pytest
from unittest.mock import MagicMock, patch, ANY
import json
import uuid
from app.lambda_function import lambda_handler
from app.db_utils import update_contactabilidad_fields

# Mock Constants
CLIENT_ID = str(uuid.uuid4())
CONTACTABILIDAD_CONFIG = {
    "db_schema": "public",
    "db_table": "contactabilidad"
}
ALLOWED_FIELDS = {"requerido_correo", "requerido_notificacion", "requerido_celular"}

@pytest.fixture
def mock_db_connection():
    with patch('app.lambda_function.conexion_bd') as mock_conn_fn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_fn.return_value = mock_conn
        yield mock_conn, mock_cursor

# --- Tests for db_utils.py ---

def test_update_single_field_success():
    """Test successful dynamic update of a single field."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    fields_to_update = {"requerido_correo": True}

    rows = update_contactabilidad_fields(mock_cursor, CONTACTABILIDAD_CONFIG, CLIENT_ID, fields_to_update)

    # Check that the SQL is correctly formatted for one field
    mock_cursor.execute.assert_called_once_with(
        'UPDATE "public"."contactabilidad" SET "requerido_correo" = %s WHERE id_cliente = %s',
        (True, CLIENT_ID)
    )
    assert rows == 1

def test_update_multiple_fields_success():
    """Test successful dynamic update of multiple fields."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    fields_to_update = {"requerido_notificacion": False, "requerido_celular": True}

    rows = update_contactabilidad_fields(mock_cursor, CONTACTABILIDAD_CONFIG, CLIENT_ID, fields_to_update)

    # The order of fields in the dictionary is not guaranteed, so we check for both possible SQL queries
    expected_sql_1 = 'UPDATE "public"."contactabilidad" SET "requerido_notificacion" = %s, "requerido_celular" = %s WHERE id_cliente = %s'
    expected_sql_2 = 'UPDATE "public"."contactabilidad" SET "requerido_celular" = %s, "requerido_notificacion" = %s WHERE id_cliente = %s'

    try:
        mock_cursor.execute.assert_called_with(expected_sql_1, (False, True, CLIENT_ID))
    except AssertionError:
        mock_cursor.execute.assert_called_with(expected_sql_2, (True, False, CLIENT_ID))

    assert rows == 1

def test_update_with_invalid_field_ignored():
    """Test that invalid fields in the update dictionary are ignored."""
    mock_cursor = MagicMock()
    fields_to_update = {"requerido_correo": True, "invalid_field": "test"}

    update_contactabilidad_fields(mock_cursor, CONTACTABILIDAD_CONFIG, CLIENT_ID, fields_to_update)

    # Only the valid field should be in the query
    mock_cursor.execute.assert_called_once_with(
        'UPDATE "public"."contactabilidad" SET "requerido_correo" = %s WHERE id_cliente = %s',
        (True, CLIENT_ID)
    )

def test_update_with_no_valid_fields_raises_error():
    """Test that a ValueError is raised if no valid fields are provided."""
    mock_cursor = MagicMock()
    fields_to_update = {"invalid_field_1": True, "invalid_field_2": False}

    with pytest.raises(ValueError, match="No se proporcionaron campos válidos para actualizar."):
        update_contactabilidad_fields(mock_cursor, CONTACTABILIDAD_CONFIG, CLIENT_ID, fields_to_update)

    mock_cursor.execute.assert_not_called()

# --- Tests for lambda_function.py ---

# Import the internal function to test it directly
from app.lambda_function import _validate_contactabilidad_patch_body

def test_validate_body_success_single_field():
    """Test validator with a single valid field."""
    body = {"requerido_correo": True}
    fields, error = _validate_contactabilidad_patch_body(body)
    assert error is None
    assert fields == body

def test_validate_body_success_multiple_fields():
    """Test validator with multiple valid fields."""
    body = {"requerido_correo": True, "requerido_notificacion": False}
    fields, error = _validate_contactabilidad_patch_body(body)
    assert error is None
    assert fields == body

def test_validate_body_empty():
    """Test validator with an empty body."""
    fields, error = _validate_contactabilidad_patch_body({})
    assert fields is None
    assert "JSON no vacío" in error

def test_validate_body_invalid_key():
    """Test validator with an invalid key."""
    fields, error = _validate_contactabilidad_patch_body({"invalid_key": True})
    assert fields is None
    assert "no es actualizable" in error

def test_validate_body_non_boolean_value():
    """Test validator with a non-boolean value."""
    fields, error = _validate_contactabilidad_patch_body({"requerido_correo": "true"})
    assert fields is None
    assert "debe ser un booleano" in error

def test_validate_body_mixed_valid_and_invalid():
    """Test validator with mixed valid and invalid keys."""
    body = {"requerido_correo": True, "invalid_key": False}
    fields, error = _validate_contactabilidad_patch_body(body)
    assert fields is None
    assert "no es actualizable" in error

def create_patch_event(client_id, body):
    return {
        "requestContext": {
            "http": { "method": "PATCH", "path": f"/contactabilidad/{client_id}/requerido" }
        },
        "body": json.dumps(body)
    }

def test_patch_single_field_success(mock_db_connection):
    """Test successful PATCH with a single valid field."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.rowcount = 1
    event = create_patch_event(CLIENT_ID, {"requerido_correo": True})

    response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    mock_conn.commit.assert_called_once()

def test_patch_multiple_fields_success(mock_db_connection):
    """Test successful PATCH with multiple valid fields."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.rowcount = 1
    body = {"requerido_notificacion": True, "requerido_celular": False}
    event = create_patch_event(CLIENT_ID, body)

    response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    mock_conn.commit.assert_called_once()

def test_patch_client_not_found(mock_db_connection):
    """Test PATCH when the client_id is not found."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.rowcount = 0
    event = create_patch_event(CLIENT_ID, {"requerido_correo": True})

    response = lambda_handler(event, None)

    assert response['statusCode'] == 404
    assert 'No se encontró un registro' in json.loads(response['body'])['mensaje']
    mock_conn.rollback.assert_called_once()

def test_patch_invalid_field_name(mock_db_connection):
    """Test PATCH with an invalid field name in the body."""
    event = create_patch_event(CLIENT_ID, {"requerido_sms": True})
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400
    assert 'no es actualizable' in json.loads(response['body'])['mensaje']

def test_patch_non_boolean_value(mock_db_connection):
    """Test PATCH with a non-boolean value."""
    event = create_patch_event(CLIENT_ID, {"requerido_correo": "true"})
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400
    assert 'debe ser un booleano' in json.loads(response['body'])['mensaje']

def test_patch_empty_body(mock_db_connection):
    """Test PATCH with an empty JSON object as the body."""
    event = create_patch_event(CLIENT_ID, {})
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400
    assert 'JSON no vacío' in json.loads(response['body'])['mensaje']

def test_patch_invalid_uuid(mock_db_connection):
    """Test PATCH request with an invalid UUID."""
    event = create_patch_event("invalid-uuid", {"requerido_correo": True})
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400
    assert 'ID de cliente no válido' in json.loads(response['body'])['mensaje']
