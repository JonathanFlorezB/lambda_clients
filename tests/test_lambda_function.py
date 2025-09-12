import json
import pytest
from unittest.mock import patch, MagicMock
from app.lambda_function import lambda_handler

@pytest.fixture
def mock_db_connection():
    """Fixture to mock the database connection and cursor."""
    with patch('app.lambda_function.conexion_bd') as mock_conexion:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conexion.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        yield mock_cursor

@pytest.fixture
def api_gateway_event():
    """Fixture for a sample API Gateway event."""
    def _create_event(method, path, query_params=None, body=None):
        event = {
            "requestContext": {
                "http": {
                    "method": method,
                    "path": path
                }
            },
            "queryStringParameters": query_params or {},
            "body": json.dumps(body) if body else "{}"
        }
        return event
    return _create_event

# Tests for GET /clientes
def test_get_clientes_success(mock_db_connection, api_gateway_event):
    """Test successful GET request to /clientes."""
    event = api_gateway_event("GET", "/clientes")

    # Mock the return value of get_paginated_data
    with patch('app.lambda_function.get_paginated_data') as mock_get_data:
        mock_get_data.return_value = {"data": [], "pagination": {}}

        response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "data" in body
        assert "pagination" in body
        mock_get_data.assert_called_once()

# Test for POST /clientes
def test_post_clientes_success(mock_db_connection, api_gateway_event):
    """Test successful POST request to /clientes."""
    client_data = [{"accion": "A", "codigo_identificacion": "C001"}]
    event = api_gateway_event("POST", "/clientes", body=client_data)

    with patch('app.lambda_function.validate_and_process_client_data') as mock_process_data:
        mock_process_data.return_value = (1, 0, 0, []) # inserted, updated, deleted, errors

        response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["inserted_count"] == 1
        mock_process_data.assert_called_once()

# Test for POST /clientes/validaciones
def test_post_clientes_validaciones_success(mock_db_connection, api_gateway_event):
    """Test successful POST request to /clientes/validaciones."""
    client_data = [{"accion": "A", "codigo_identificacion": "C001"}]
    event = api_gateway_event("POST", "/clientes/validaciones", body=client_data)

    with patch('app.lambda_function.validate_data') as mock_validate_data:
        mock_validate_data.return_value = (1, []) # valid_count, errors

        response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["valid_count"] == 1
        mock_validate_data.assert_called_once()

# Test for GET /informacionCliente/{id_cliente}
def test_get_informacion_cliente_success(mock_db_connection, api_gateway_event):
    """Test successful GET request to /informacionCliente/{id_cliente}."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    event = api_gateway_event("GET", f"/informacionCliente/{client_id}")

    with patch('app.lambda_function.get_client_info') as mock_get_info:
        mock_get_info.return_value = {"contactabilidad": {"id": 1}, "productos": []}

        response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "contactabilidad" in body
        mock_get_info.assert_called_once()

def test_get_informacion_cliente_not_found(mock_db_connection, api_gateway_event):
    """Test GET request to /informacionCliente/{id_cliente} when client is not found."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    event = api_gateway_event("GET", f"/informacionCliente/{client_id}")

    with patch('app.lambda_function.get_client_info') as mock_get_info:
        mock_get_info.return_value = {"contactabilidad": None, "productos": []}

        response = lambda_handler(event, {})

        assert response["statusCode"] == 404

# Test for PATCH /contactabilidad/{id_cliente}/requerido
def test_patch_contactabilidad_success(mock_db_connection, api_gateway_event):
    """Test successful PATCH to /contactabilidad/{id_cliente}/requerido."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    body_data = {"requerido_correo": True}
    event = api_gateway_event("PATCH", f"/contactabilidad/{client_id}/requerido", body=body_data)

    with patch('app.lambda_function.update_contactabilidad_fields') as mock_update:
        mock_update.return_value = 1 # rows_affected

        response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        mock_update.assert_called_once()

# Test for invalid path
def test_invalid_path_returns_404(mock_db_connection, api_gateway_event):
    """Test that an invalid path returns a 404 Not Found response."""
    event = api_gateway_event("GET", "/invalid/path")
    response = lambda_handler(event, {})
    assert response["statusCode"] == 404

# Test for internal server error
def test_internal_server_error(mock_db_connection, api_gateway_event):
    """Test that a generic exception results in a 500 Internal Server Error."""
    event = api_gateway_event("GET", "/clientes")

    with patch('app.lambda_function.get_paginated_data', side_effect=Exception("DB error")):
        response = lambda_handler(event, {})
        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["mensaje"] == 'Error interno del servidor'

# Test for invalid body in POST
def test_invalid_body_post_clientes(mock_db_connection, api_gateway_event):
    """Test POST to /clientes with an invalid body (not a list)."""
    event = api_gateway_event("POST", "/clientes", body={"not": "a list"})

    response = lambda_handler(event, {})

    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "El body debe ser una lista de diccionarios" in body["mensaje"]

# More tests for handle_client_info_resources
def test_get_info_missing_client_id(mock_db_connection, api_gateway_event):
    """Test GET to /informacionCliente without a client ID."""
    event = api_gateway_event("GET", "/informacionCliente/")
    response = lambda_handler(event, {})
    assert response["statusCode"] == 400
    assert "Se requiere id_cliente" in json.loads(response["body"])["mensaje"]

def test_get_info_invalid_client_id(mock_db_connection, api_gateway_event):
    """Test GET to /informacionCliente with an invalid UUID."""
    event = api_gateway_event("GET", "/informacionCliente/invalid-uuid")
    response = lambda_handler(event, {})
    assert response["statusCode"] == 400
    assert "ID de cliente no válido" in json.loads(response["body"])["mensaje"]

# Test for GET /historial_transaccion/{id_cliente}
def test_get_historial_transaccion_success(mock_db_connection, api_gateway_event):
    """Test successful GET request to /historial_transaccion/{id_cliente}."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    event = api_gateway_event("GET", f"/historial_transaccion/{client_id}")

    with patch('app.lambda_function.get_paginated_data') as mock_get_data:
        mock_get_data.return_value = {"data": [{"id": 1}], "pagination": {"totalRecords": 1}}

        response = lambda_handler(event, {})

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["pagination"]["totalRecords"] == 1
        mock_get_data.assert_called_once()

def test_handle_clientes_resource_404(mock_db_connection, api_gateway_event):
    """Test that a non-GET/POST method to /clientes returns 404."""
    event = api_gateway_event("PUT", "/clientes")
    response = lambda_handler(event, {})
    assert response["statusCode"] == 404

def test_handle_client_data_post_207_status(mock_db_connection, api_gateway_event):
    """Test POST to /clientes that results in a 207 Multi-Status."""
    client_data = [{"accion": "A", "codigo_identificacion": "C001"}, {"accion": "M", "codigo_identificacion": "FAIL"}]
    event = api_gateway_event("POST", "/clientes", body=client_data)

    with patch('app.lambda_function.validate_and_process_client_data') as mock_process_data:
        # Simulate one success and one error
        mock_process_data.return_value = (1, 0, 0, [{"fila": 2, "errors": "some error"}])

        response = lambda_handler(event, {})

        assert response["statusCode"] == 207
        body = json.loads(response["body"])
        assert body["inserted_count"] == 1
        assert len(body["errors"]) == 1

def test_options_request(api_gateway_event):
    """Test that an OPTIONS request returns a 200 response."""
    event = api_gateway_event("OPTIONS", "/")
    response = lambda_handler(event, {})
    assert response["statusCode"] == 200

def test_post_clientes_invalid_action(mock_db_connection, api_gateway_event):
    """Test POST to /clientes with an invalid action."""
    client_data = [{"accion": "X", "codigo_identificacion": "C001"}]
    event = api_gateway_event("POST", "/clientes", body=client_data)

    response = lambda_handler(event, {})

    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "Cada registro debe tener una acción válida" in body["mensaje"]

def test_get_historial_transaccion_not_found(mock_db_connection, api_gateway_event):
    """Test GET to /historial_transaccion when no history is found."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    event = api_gateway_event("GET", f"/historial_transaccion/{client_id}")

    with patch('app.lambda_function.get_paginated_data') as mock_get_data:
        mock_get_data.return_value = {"data": [], "pagination": {"totalRecords": 0}}

        response = lambda_handler(event, {})

        assert response["statusCode"] == 404

def test_client_info_resource_unsupported_method(mock_db_connection, api_gateway_event):
    """Test an unsupported method to /informacionCliente returns 404."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    event = api_gateway_event("PUT", f"/informacionCliente/{client_id}")
    response = lambda_handler(event, {})
    assert response["statusCode"] == 404

@pytest.mark.parametrize("body_data, expected_message", [
    ({}, "El cuerpo de la solicitud debe ser un objeto JSON no vacío."),
    ({"invalid_field": True}, 'El campo "invalid_field" no es actualizable.'),
    ({"requerido_correo": "not-a-boolean"}, 'El valor para "requerido_correo" debe ser un booleano.'),
    ({"invalid_field": True, "another_invalid": "abc"}, 'El campo "invalid_field" no es actualizable.')
])
def test_patch_contactabilidad_invalid_body(mock_db_connection, api_gateway_event, body_data, expected_message):
    """Test PATCH to /contactabilidad with various invalid bodies."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    event = api_gateway_event("PATCH", f"/contactabilidad/{client_id}/requerido", body=body_data)
    response = lambda_handler(event, {})
    assert response["statusCode"] == 400
    assert expected_message in json.loads(response["body"])["mensaje"]

def test_patch_contactabilidad_zero_rows_affected(mock_db_connection, api_gateway_event):
    """Test PATCH to /contactabilidad that affects zero rows."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    body_data = {"requerido_correo": True}
    event = api_gateway_event("PATCH", f"/contactabilidad/{client_id}/requerido", body=body_data)

    with patch('app.lambda_function.update_contactabilidad_fields') as mock_update:
        mock_update.return_value = 0 # rows_affected

        response = lambda_handler(event, {})

        assert response["statusCode"] == 404

def test_patch_contactabilidad_invalid_uuid(mock_db_connection, api_gateway_event):
    """Test PATCH to /contactabilidad with an invalid client UUID."""
    event = api_gateway_event("PATCH", "/contactabilidad/invalid-uuid/requerido", body={"requerido_correo": True})
    response = lambda_handler(event, {})
    assert response["statusCode"] == 400
    assert "ID de cliente no válido" in json.loads(response["body"])["mensaje"]

def test_handle_contactabilidad_resource_404(mock_db_connection, api_gateway_event):
    """Test that a non-PATCH method to /contactabilidad/.../requerido returns 404."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    event = api_gateway_event("GET", f"/contactabilidad/{client_id}/requerido")
    response = lambda_handler(event, {})
    assert response["statusCode"] == 404

def test_patch_contactabilidad_value_error(mock_db_connection, api_gateway_event):
    """Test PATCH to /contactabilidad that raises a ValueError from the update function."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    # This body is valid, so it will pass the initial validation
    body_data = {"requerido_correo": True}
    event = api_gateway_event("PATCH", f"/contactabilidad/{client_id}/requerido", body=body_data)

    with patch('app.lambda_function.update_contactabilidad_fields') as mock_update:
        # We mock the call that happens *after* validation to raise the error
        mock_update.side_effect = ValueError("Forced DB error")

        response = lambda_handler(event, {})

        assert response["statusCode"] == 400
        assert "Forced DB error" in json.loads(response["body"])["mensaje"]

def test_validate_contactabilidad_no_valid_fields(mock_db_connection, api_gateway_event):
    """Test _validate_contactabilidad_patch_body with no valid fields."""
    client_id = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    body_data = {"another_invalid_field": "some_value"}
    event = api_gateway_event("PATCH", f"/contactabilidad/{client_id}/requerido", body=body_data)

    # This test will hit the `if not fields_to_update:` line in _validate_contactabilidad_patch_body
    response = lambda_handler(event, {})
    assert response["statusCode"] == 400
    assert 'El campo "another_invalid_field" no es actualizable.' in json.loads(response["body"])["mensaje"]

def test_post_clientes_validaciones_invalid_action(mock_db_connection, api_gateway_event):
    """Test POST to /clientes/validaciones with an invalid action."""
    client_data = [{"accion": "X", "codigo_identificacion": "C001"}]
    event = api_gateway_event("POST", "/clientes/validaciones", body=client_data)

    response = lambda_handler(event, {})

    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "Cada registro debe tener una acción válida" in body["mensaje"]
