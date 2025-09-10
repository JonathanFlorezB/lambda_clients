import pytest
from unittest.mock import MagicMock, patch, ANY
import json
import uuid
from app.lambda_function import lambda_handler
from app.db_utils import update_contactabilidad_requerido

# Mock Constants
CLIENT_ID = str(uuid.uuid4())
CONTACTABILIDAD_CONFIG = {
    "db_schema": "public",
    "db_table": "contactabilidad"
}

@pytest.fixture
def mock_db_connection():
    with patch('app.lambda_function.conexion_bd') as mock_conn_fn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_fn.return_value = mock_conn
        yield mock_conn, mock_cursor

# --- Tests for db_utils.py ---

def test_update_contactabilidad_requerido_success():
    """Test successful update of the 'requerido' field."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1

    rows_affected = update_contactabilidad_requerido(
        mock_cursor, CONTACTABILIDAD_CONFIG, CLIENT_ID, True
    )

    expected_sql = 'UPDATE "public"."contactabilidad" SET requerido = %s WHERE id_cliente = %s'
    mock_cursor.execute.assert_called_once_with(expected_sql, (True, CLIENT_ID))
    assert rows_affected == 1

def test_update_contactabilidad_requerido_not_found():
    """Test update when no record is found."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0

    rows_affected = update_contactabilidad_requerido(
        mock_cursor, CONTACTABILIDAD_CONFIG, CLIENT_ID, False
    )

    expected_sql = 'UPDATE "public"."contactabilidad" SET requerido = %s WHERE id_cliente = %s'
    mock_cursor.execute.assert_called_once_with(expected_sql, (False, CLIENT_ID))
    assert rows_affected == 0

def test_update_contactabilidad_requerido_db_error():
    """Test handling of a database exception."""
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("DB Error")

    with pytest.raises(Exception, match="DB Error"):
        update_contactabilidad_requerido(
            mock_cursor, CONTACTABILIDAD_CONFIG, CLIENT_ID, True
        )

# --- Tests for lambda_function.py ---

def create_patch_event(client_id, body):
    return {
        "requestContext": {
            "http": {
                "method": "PATCH",
                "path": f"/contactabilidad/{client_id}/requerido"
            }
        },
        "body": json.dumps(body)
    }

def test_patch_requerido_success(mock_db_connection):
    """Test successful PATCH request to update 'requerido'."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.rowcount = 1

    event = create_patch_event(CLIENT_ID, {"requerido": True})
    response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert 'actualizado correctamente' in body['mensaje']
    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()

def test_patch_requerido_client_not_found(mock_db_connection):
    """Test PATCH request when the client_id is not found."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.rowcount = 0  # Simulate no rows affected

    event = create_patch_event(CLIENT_ID, {"requerido": False})
    response = lambda_handler(event, None)

    assert response['statusCode'] == 404
    body = json.loads(response['body'])
    assert 'No se encontró un registro' in body['mensaje']
    mock_conn.commit.assert_not_called()
    mock_conn.rollback.assert_called_once()

def test_patch_requerido_invalid_uuid(mock_db_connection):
    """Test PATCH request with an invalid UUID."""
    event = create_patch_event("invalid-uuid", {"requerido": True})
    response = lambda_handler(event, None)

    assert response['statusCode'] == 400
    body = json.loads(response['body'])
    assert 'ID de cliente no válido' in body['mensaje']

def test_patch_requerido_missing_body_field(mock_db_connection):
    """Test PATCH request with the 'requerido' field missing from the body."""
    event = create_patch_event(CLIENT_ID, {})
    response = lambda_handler(event, None)

    assert response['statusCode'] == 400
    body = json.loads(response['body'])
    assert 'obligatorio y debe ser un booleano' in body['mensaje']

def test_patch_requerido_invalid_body_type(mock_db_connection):
    """Test PATCH request with a non-boolean value for 'requerido'."""
    event = create_patch_event(CLIENT_ID, {"requerido": "not-a-boolean"})
    response = lambda_handler(event, None)

    assert response['statusCode'] == 400
    body = json.loads(response['body'])
    assert 'obligatorio y debe ser un booleano' in body['mensaje']

def test_patch_requerido_wrong_path(mock_db_connection):
    """Test PATCH request to a non-existent sub-path."""
    event = {
        "requestContext": {
            "http": {
                "method": "PATCH",
                "path": f"/contactabilidad/{CLIENT_ID}/wrong_path"
            }
        },
        "body": json.dumps({"requerido": True})
    }
    response = lambda_handler(event, None)

    assert response['statusCode'] == 404
    body = json.loads(response['body'])
    assert 'Recurso no encontrado' in body['mensaje']
