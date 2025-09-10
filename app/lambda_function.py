import json
import uuid
import os
import sys
from app.logger_config import logger
from app.config import RESOURCES
from app.db_utils import (
    get_client_info,
    get_paginated_data,
    validate_and_process_client_data,
    validate_data,
    update_contactabilidad_fields
)
from app.db_connection import conexion_bd
from app.shared_utils import build_response

# Constants for error messages
RESOURCE_NOT_FOUND_MSG = 'Recurso no encontrado'
CLIENT_ID_REQUIRED_MSG = 'Se requiere id_cliente en la URL'
INVALID_CLIENT_ID_MSG = 'ID de cliente no válido'
NO_CLIENT_INFO_MSG = 'No se encontró información del cliente'
NO_TRANSACTION_HISTORY_MSG = 'No se encontró historial'
BODY_MUST_BE_LIST_MSG = 'El body debe ser una lista de diccionarios'
INVALID_ACTION_MSG = 'Cada registro debe tener una acción válida (A, E, M)'
INTERNAL_ERROR_MSG = 'Error interno del servidor'

def handle_get_clientes(cursor, query_params):
    """Maneja la solicitud GET para el recurso 'clientes'."""
    return get_paginated_data(cursor, RESOURCES["clientes"], query_params)

def handle_post_clientes(cursor, body):
    """Maneja la solicitud POST para el recurso 'clientes'."""
    data = json.loads(body)
    if not isinstance(data, list):
        raise ValueError(BODY_MUST_BE_LIST_MSG)
    
    for item in data:
        if 'accion' not in item or item['accion'] not in ['A', 'E', 'M']:
            raise ValueError(INVALID_ACTION_MSG)
    
    return validate_and_process_client_data(cursor, RESOURCES["clientes"], data)

def handle_post_validaciones(cursor, body):
    """Maneja la solicitud POST para el subrecurso 'clientes/validaciones'."""
    data = json.loads(body)
    if not isinstance(data, list):
        raise ValueError(BODY_MUST_BE_LIST_MSG)
    
    for item in data:
        if 'accion' not in item or item['accion'] not in ['A', 'E', 'M']:
            raise ValueError(INVALID_ACTION_MSG)
    
    return validate_data(cursor, RESOURCES["clientes"], data)

def lambda_handler(event, context):
    """Manejador principal de la función Lambda."""
    method = event.get('requestContext', {}).get('http', {}).get('method')
    path = event.get('requestContext', {}).get('http', {}).get('path', '')
    
    if method == "OPTIONS":
        return build_response(200, {})

    return handle_main_request(event, method, path)

def handle_main_request(event, method, path):
    """Maneja la lógica principal de la solicitud"""
    path_parts = path.strip('/').split('/')
    resource = path_parts[0] if path_parts else ''
    
    conn = None
    try:
        logger.info(f"Iniciando petición {method} para {path}...")
        conn = conexion_bd()
        cursor = conn.cursor()

        if resource in ['informacionCliente', 'historial_transaccion']:
            return handle_client_info_resources(cursor, event, method, path_parts, resource)
        elif resource == "clientes":
            return handle_clientes_resource(conn, cursor, event, method, path_parts)
        elif resource == "contactabilidad":
            return handle_contactabilidad_resource(conn, cursor, event, method, path_parts)
        
        return build_response(404, {'mensaje': RESOURCE_NOT_FOUND_MSG})

    except ValueError as ve:
        return build_response(400, {'mensaje': str(ve)})
    except Exception as e:
        logger.error(f"Error inesperado: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return build_response(500, {'mensaje': INTERNAL_ERROR_MSG})
    finally:
        if conn:
            cursor.close()
            conn.close()

def handle_client_info_resources(cursor, event, method, path_parts, resource):
    """Maneja los recursos de información de cliente"""
    if len(path_parts) < 2:
        return build_response(400, {'mensaje': CLIENT_ID_REQUIRED_MSG})
    
    id_cliente = path_parts[1]
    try:
        uuid.UUID(id_cliente)
    except ValueError:
        return build_response(400, {'mensaje': INVALID_CLIENT_ID_MSG})

    if resource == "informacionCliente" and method == 'GET':
        return handle_client_info(cursor, id_cliente)
    elif resource == "historial_transaccion" and method == 'GET':
        return handle_transaction_history(cursor, event, id_cliente)
    
    return build_response(404, {'mensaje': RESOURCE_NOT_FOUND_MSG})

def handle_client_info(cursor, id_cliente):
    """Maneja la obtención de información del cliente"""
    client_info = get_client_info(cursor, RESOURCES["contactabilidad"], RESOURCES["productos"], id_cliente)
    if not client_info["contactabilidad"] and not client_info["productos"]:
        return build_response(404, {'mensaje': NO_CLIENT_INFO_MSG})
    return build_response(200, client_info)

def handle_transaction_history(cursor, event, id_cliente):
    """Maneja la obtención del historial de transacciones"""
    query_params = event.get('queryStringParameters', {})
    query_params['id_cliente'] = id_cliente
    response_body = get_paginated_data(cursor, RESOURCES["historial_transacciones"], query_params)
    if response_body["pagination"]["totalRecords"] == 0:
        return build_response(404, {'mensaje': NO_TRANSACTION_HISTORY_MSG})
    return build_response(200, response_body)

def handle_clientes_resource(conn, cursor, event, method, path_parts):
    """Maneja el recurso clientes"""
    subpath = path_parts[1] if len(path_parts) > 1 else ''
    
    if method == 'GET' and not subpath:
        response_body = handle_get_clientes(cursor, event.get('queryStringParameters', {}))
        return build_response(200, response_body)
    elif method == 'POST':
        return handle_clientes_post(conn, cursor, event, subpath)
    
    return build_response(404, {'mensaje': RESOURCE_NOT_FOUND_MSG})

def handle_clientes_post(conn, cursor, event, subpath):
    """Maneja las solicitudes POST al recurso clientes"""
    body_str = event.get('body', '[]')
    
    if subpath == "validaciones":
        return handle_validation_post(cursor, body_str)
    else:
        return handle_client_data_post(conn, cursor, body_str)

def handle_validation_post(cursor, body_str):
    """Maneja las validaciones de clientes"""
    valid_count, errors = handle_post_validaciones(cursor, body_str)
    response_body = {
        'valid_count': valid_count,
        'mensaje': f'{valid_count} registros válidos de {len(json.loads(body_str))}.',
        'errors': errors
    }
    status_code = 200 if valid_count > 0 or not errors else 400
    return build_response(status_code, response_body)

def handle_contactabilidad_resource(conn, cursor, event, method, path_parts):
    """Maneja las solicitudes para el recurso de contactabilidad."""
    if method == 'PATCH' and len(path_parts) == 3 and path_parts[2] == 'requerido':
        id_cliente = path_parts[1]
        try:
            uuid.UUID(id_cliente)
        except ValueError:
            return build_response(400, {'mensaje': 'ID de cliente no válido.'})

        body = json.loads(event.get('body', '{}'))
        if not isinstance(body, dict) or not body:
            return build_response(400, {'mensaje': 'El cuerpo de la solicitud debe ser un objeto JSON no vacío.'})

        ALLOWED_FIELDS = {"requerido_correo", "requerido_notificacion", "requerido_celular"}

        # Validar que todas las llaves en el body sean permitidas y sus valores booleanos
        fields_to_update = {}
        for key, value in body.items():
            if key not in ALLOWED_FIELDS:
                return build_response(400, {'mensaje': f'El campo "{key}" no es actualizable.'})
            if not isinstance(value, bool):
                return build_response(400, {'mensaje': f'El valor para "{key}" debe ser un booleano.'})
            fields_to_update[key] = value

        if not fields_to_update:
             return build_response(400, {'mensaje': 'No se proporcionaron campos válidos para actualizar.'})

        rows_affected = update_contactabilidad_fields(cursor, RESOURCES["contactabilidad"], id_cliente, fields_to_update)

        if rows_affected > 0:
            conn.commit()
            return build_response(200, {'mensaje': 'El estado de requerido ha sido actualizado correctamente.'})
        else:
            conn.rollback()
            return build_response(404, {'mensaje': 'No se encontró un registro de contactabilidad para el cliente especificado.'})

    return build_response(404, {'mensaje': RESOURCE_NOT_FOUND_MSG})

def handle_client_data_post(conn, cursor, body_str):
    """Maneja la inserción/actualización de datos de clientes"""
    inserted_count, updated_count, deleted_count, errors = handle_post_clientes(cursor, body_str)
    conn.commit()
    response_body = {
        'inserted_count': inserted_count,
        'updated_count': updated_count,
        'deleted_count': deleted_count,
        'mensaje': f'Procesados {len(json.loads(body_str))} registros.',
        'errors': errors
    }
    status_code = 200 if (inserted_count > 0 or updated_count > 0 or deleted_count > 0) else 400
    if errors:
        status_code = 207 if (inserted_count > 0 or updated_count > 0 or deleted_count > 0) else 400
    return build_response(status_code, response_body)
