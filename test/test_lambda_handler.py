import pytest
from unittest.mock import MagicMock, patch
from app.lambda_function import (
    lambda_handler,
    handle_main_request,
    handle_client_info_resources,
    handle_client_info,
    handle_transaction_history,
    handle_clientes_resource,
    RESOURCE_NOT_FOUND_MSG,
    CLIENT_ID_REQUIRED_MSG,
    INVALID_CLIENT_ID_MSG,
    NO_CLIENT_INFO_MSG,
    NO_TRANSACTION_HISTORY_MSG
)
import uuid
import json

# Mock classes
class MockCursor:
    def __init__(self):
        self.execute = MagicMock()
        self.fetchone = MagicMock(return_value=[1])
        self.fetchall = MagicMock(return_value=[('data1', 'data2')])
        self.close = MagicMock()
        self.description = [('column1',), ('column2',)]
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class MockConnection:
    def cursor(self):
        return MockCursor()
    
    def commit(self):
        pass
    
    def rollback(self):
        pass
    
    def close(self):
        pass

# Fixtures
@pytest.fixture
def mock_db_connection():
    with patch('app.lambda_function.conexion_bd') as mock_conn:
        mock_conn.return_value = MockConnection()
        yield mock_conn

@pytest.fixture
def options_event():
    return {
        "requestContext": {
            "http": {
                "method": "OPTIONS",
                "path": "/"
            }
        }
    }

@pytest.fixture
def client_info_event():
    return {
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/informacionCliente/123e4567-e89b-12d3-a456-426614174000"
            }
        }
    }

@pytest.fixture
def transaction_history_event():
    return {
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/historial_transaccion/123e4567-e89b-12d3-a456-426614174000"
            }
        },
        "queryStringParameters": {
            "page": "1",
            "per_page": "10"
        }
    }

@pytest.fixture
def clientes_get_event():
    return {
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/clientes"
            }
        },
        "queryStringParameters": {
            "page": "1",
            "per_page": "10"
        }
    }

@pytest.fixture
def clientes_post_event():
    return {
        "requestContext": {
            "http": {
                "method": "POST",
                "path": "/clientes"
            }
        },
        "body": json.dumps([{"accion": "A", "codigo_identificacion": "123"}])
    }

# Tests
def test_lambda_handler_options(mock_db_connection, options_event):
    response = lambda_handler(options_event, None)
    assert response['statusCode'] == 200
    assert response['body'] == '{}'

def test_lambda_handler_main_request(mock_db_connection, client_info_event):
    with patch('app.lambda_function.handle_main_request') as mock_handler:
        mock_handler.return_value = {'statusCode': 200, 'body': '{}'}
        response = lambda_handler(client_info_event, None)
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_main_request_client_info(mock_db_connection, client_info_event):
    with patch('app.lambda_function.handle_client_info_resources') as mock_handler:
        mock_handler.return_value = {'statusCode': 200, 'body': '{}'}
        response = handle_main_request(
            client_info_event,
            "GET",
            "/informacionCliente/123e4567-e89b-12d3-a456-426614174000"
        )
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_main_request_clientes(mock_db_connection, clientes_get_event):
    with patch('app.lambda_function.handle_clientes_resource') as mock_handler:
        mock_handler.return_value = {'statusCode': 200, 'body': '{}'}
        response = handle_main_request(
            clientes_get_event,
            "GET",
            "/clientes"
        )
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_main_request_not_found(mock_db_connection):
    event = {
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/unknown"
            }
        }
    }
    response = handle_main_request(event, "GET", "/unknown")
    assert response['statusCode'] == 404
    assert RESOURCE_NOT_FOUND_MSG in response['body']

def test_handle_main_request_value_error(mock_db_connection):
    event = {
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/clientes"
            }
        }
    }
    with patch('app.lambda_function.handle_clientes_resource', side_effect=ValueError("Test error")):
        response = handle_main_request(event, "GET", "/clientes")
        assert response['statusCode'] == 400
        assert "Test error" in response['body']

def test_handle_main_request_exception(mock_db_connection):
    event = {
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/clientes"
            }
        }
    }
    with patch('app.lambda_function.handle_clientes_resource', side_effect=Exception("Test error")):
        response = handle_main_request(event, "GET", "/clientes")
        assert response['statusCode'] == 500

def test_handle_client_info_resources_missing_id():
    response = handle_client_info_resources(
        MagicMock(),  # cursor
        {},  # event
        "GET",  # method
        ["informacionCliente"],  # path_parts
        "informacionCliente"  # resource
    )
    assert response['statusCode'] == 400
    assert CLIENT_ID_REQUIRED_MSG in response['body']

def test_handle_client_info_resources_invalid_id():
    response = handle_client_info_resources(
        MagicMock(),  # cursor
        {},  # event
        "GET",  # method
        ["informacionCliente", "invalid-uuid"],  # path_parts
        "informacionCliente"  # resource
    )
    assert response['statusCode'] == 400

def test_handle_client_info_resources_client_info(mock_db_connection):
    with patch('app.lambda_function.handle_client_info') as mock_handler:
        mock_handler.return_value = {'statusCode': 200, 'body': '{}'}
        client_id = str(uuid.uuid4())
        response = handle_client_info_resources(
            MagicMock(),  # cursor
            {},  # event
            "GET",  # method
            ["informacionCliente", client_id],  # path_parts
            "informacionCliente"  # resource
        )
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_client_info_resources_transaction_history(mock_db_connection):
    with patch('app.lambda_function.handle_transaction_history') as mock_handler:
        mock_handler.return_value = {'statusCode': 200, 'body': '{}'}
        client_id = str(uuid.uuid4())
        response = handle_client_info_resources(
            MagicMock(),  # cursor
            {},  # event
            "GET",  # method
            ["historial_transaccion", client_id],  # path_parts
            "historial_transaccion"  # resource
        )
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_client_info_not_found(mock_db_connection):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [None, None]  # No contactabilidad or productos
    response = handle_client_info(cursor, str(uuid.uuid4()))
    assert response['statusCode'] == 404

def test_handle_transaction_history_not_found(mock_db_connection):
    cursor = MagicMock()
    cursor.fetchone.return_value = [0]  # No records found
    event = {"queryStringParameters": {}}
    response = handle_transaction_history(cursor, event, str(uuid.uuid4()))
    assert response['statusCode'] == 404

def test_handle_clientes_resource_get(mock_db_connection, clientes_get_event):
    with patch('app.lambda_function.handle_get_clientes') as mock_handler:
        mock_handler.return_value = {'data': [], 'pagination': {}}
        cursor = MagicMock()
        response = handle_clientes_resource(
            MagicMock(),  # conn
            cursor,  # cursor
            clientes_get_event,  # event
            "GET",  # method
            ["clientes"]  # path_parts
        )
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_clientes_resource_post(mock_db_connection, clientes_post_event):
    with patch('app.lambda_function.handle_clientes_post') as mock_handler:
        mock_handler.return_value = {'statusCode': 200, 'body': '{}'}
        cursor = MagicMock()
        response = handle_clientes_resource(
            MagicMock(),  # conn
            cursor,  # cursor
            clientes_post_event,  # event
            "POST",  # method
            ["clientes"]  # path_parts
        )
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_clientes_resource_not_found(mock_db_connection):
    response = handle_clientes_resource(
        MagicMock(),  # conn
        MagicMock(),  # cursor
        {},  # event
        "PUT",  # method
        ["clientes"]  # path_parts
    )
    assert response['statusCode'] == 404
    assert RESOURCE_NOT_FOUND_MSG in response['body']

def test_handle_post_clientes_invalid_body():
    from app.lambda_function import handle_post_clientes
    with pytest.raises(ValueError, match='El body debe ser una lista de diccionarios'):
        handle_post_clientes(MagicMock(), json.dumps({}))

def test_handle_post_clientes_invalid_action():
    from app.lambda_function import handle_post_clientes
    with pytest.raises(ValueError, match='Cada registro debe tener una acción válida'):
        handle_post_clientes(MagicMock(), json.dumps([{"accion": "X"}]))

def test_handle_post_validaciones_invalid_body():
    from app.lambda_function import handle_post_validaciones
    with pytest.raises(ValueError, match='El body debe ser una lista de diccionarios'):
        handle_post_validaciones(MagicMock(), json.dumps({}))

def test_handle_post_validaciones_invalid_action():
    from app.lambda_function import handle_post_validaciones
    with pytest.raises(ValueError, match='Cada registro debe tener una acción válida'):
        handle_post_validaciones(MagicMock(), json.dumps([{"accion": "X"}]))

def test_handle_client_info_resources_not_get():
    client_id = str(uuid.uuid4())
    response = handle_client_info_resources(
        MagicMock(),
        {},
        "POST",
        ["informacionCliente", client_id],
        "informacionCliente"
    )
    assert response['statusCode'] == 404
    assert RESOURCE_NOT_FOUND_MSG in response['body']

@patch('app.lambda_function.get_client_info', return_value={'contactabilidad': [], 'productos': []})
def test_handle_client_info_not_found_case(mock_get_client_info):
    response = handle_client_info(MagicMock(), 'some-id')
    assert response['statusCode'] == 404
    body = json.loads(response['body'])
    assert body['mensaje'] == NO_CLIENT_INFO_MSG

@patch('app.lambda_function.get_paginated_data', return_value={'pagination': {'totalRecords': 0}})
def test_handle_transaction_history_not_found_case(mock_get_paginated_data):
    event = {'queryStringParameters': {}}
    response = handle_transaction_history(MagicMock(), event, 'some-id')
    assert response['statusCode'] == 404
    body = json.loads(response['body'])
    assert body['mensaje'] == NO_TRANSACTION_HISTORY_MSG

def test_handle_clientes_post_validaciones(mock_db_connection):
    from app.lambda_function import handle_clientes_post
    event = {
        "body": json.dumps([{"accion": "A", "codigo_identificacion": "123"}])
    }
    with patch('app.lambda_function.handle_validation_post') as mock_handler:
        mock_handler.return_value = {'statusCode': 200, 'body': '{}'}
        response = handle_clientes_post(MagicMock(), MagicMock(), event, "validaciones")
        assert response['statusCode'] == 200
        mock_handler.assert_called_once()

def test_handle_validation_post_with_errors(mock_db_connection):
    from app.lambda_function import handle_validation_post
    with patch('app.lambda_function.handle_post_validaciones', return_value=(0, ['error'])):
        response = handle_validation_post(MagicMock(), json.dumps([{}]))
        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert body['valid_count'] == 0
        assert 'errors' in body

def test_handle_client_data_post_with_errors(mock_db_connection):
    from app.lambda_function import handle_client_data_post
    with patch('app.lambda_function.handle_post_clientes', return_value=(0, 0, 0, ['error'])):
        conn = MockConnection()
        response = handle_client_data_post(conn, conn.cursor(), json.dumps([{}]))
        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert body['inserted_count'] == 0
        assert 'errors' in body

def test_handle_client_data_post_with_partial_success(mock_db_connection):
    from app.lambda_function import handle_client_data_post
    with patch('app.lambda_function.handle_post_clientes', return_value=(1, 0, 0, ['error'])):
        conn = MockConnection()
        response = handle_client_data_post(conn, conn.cursor(), json.dumps([{}, {}]))
        assert response['statusCode'] == 207
        body = json.loads(response['body'])
        assert body['inserted_count'] == 1
        assert 'errors' in body
