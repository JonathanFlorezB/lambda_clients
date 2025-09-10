import uuid
import math
import re
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_paginated_data(cursor, resource_config, query_params):
    """Función genérica para manejar GET con paginación"""
    page = int(query_params.get('page', '1')) if query_params.get(
        'page', '1').isdigit() else 1
    if page < 1:
        page = 1
    offset = (page - 1) * resource_config["limit"]
    table_name = f'"{resource_config["db_schema"]}"."{
        resource_config["db_table"]}"'

    where_clauses = []
    params = []

    # Filtros de búsqueda adicionales
    for field in resource_config["search_fields"]:
        if field in query_params:
            search_term = query_params[field].strip()
            if search_term:
                like_term = f"{search_term.lower()}%"
                where_clauses.append(f"LOWER({field}::text) LIKE %s")
                params.append(like_term)

    # Agregar filtro obligatorio por id_cliente para historial_transacciones si está presente
    if "id_cliente" in query_params:
        where_clauses.append("id_cliente = %s")
        params.append(query_params["id_cliente"])

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    count_sql = f"SELECT COUNT(*) FROM {table_name} {where_sql}"
    cursor.execute(count_sql, tuple(params))
    total_records = cursor.fetchone()[0]
    total_pages = math.ceil(
        total_records / resource_config["limit"]) if total_records > 0 else 1

    data_params = params + [resource_config["limit"], offset]
    select_sql = f"SELECT * FROM {table_name} {where_sql} ORDER BY {
        resource_config['columns'][0]} ASC LIMIT %s OFFSET %s"
    cursor.execute(select_sql, tuple(data_params))

    column_names = [desc[0] for desc in cursor.description]
    results = [dict(zip(column_names, row)) for row in cursor.fetchall()]

    return {
        "pagination": {
            "currentPage": page, "totalPages": total_pages,
            "totalRecords": total_records, "recordsPerPage": resource_config["limit"]
        },
        "data": results
    }


def validate_data(cursor, resource_config, data) -> Tuple[int, List[Dict]]:
    """Validar datos de clientes según acción (A, E, M)"""
    errors = []
    table_name = f'"{resource_config["db_schema"]}"."{
        resource_config["db_table"]}"'

    errors = validate_actions(data)
    codigo_counts = count_codigo_occurrences(data)
    errors += validate_items(cursor, data, resource_config,
                             table_name, codigo_counts, errors)

    valid_count = len(data) - len(errors)
    return valid_count, errors


def validate_actions(data: List[Dict]) -> List[Dict]:
    """Validar que todos los registros tengan acción válida"""
    errors = []
    for index, item in enumerate(data):
        if 'accion' not in item or item['accion'] not in ['A', 'E', 'M']:
            errors.append({
                "fila": index + 1,
                "errors": {"accion": "La acción debe ser A (Agregar), E (Eliminar) o M (Modificar)"}
            })
    return errors


def count_codigo_occurrences(data: List[Dict]) -> Dict[str, int]:
    """Contar ocurrencias de códigos de identificación"""
    codigo_counts = {}
    for item in data:
        if item.get('accion') in ['A', 'M', 'E']:
            codigo = item.get("codigo_identificacion", "")
            if codigo:
                codigo_counts[codigo] = codigo_counts.get(codigo, 0) + 1
    return codigo_counts


def validate_items(cursor, data: List[Dict], resource_config: Dict,
                   table_name: str, codigo_counts: Dict[str, int],
                   existing_errors: List[Dict]) -> List[Dict]:
    """Validar todos los ítems de datos"""
    errors = []
    for index, item in enumerate(data):
        if any(e["fila"] == index + 1 for e in existing_errors):
            continue

        action = item.get('accion')
        field_errors = validate_item_fields(item, action, resource_config)
        field_errors.update(validate_tipo_identificacion(item, action))
        field_errors.update(validate_fecha_afiliacion(item, action))
        field_errors.update(validate_database_operations(
            cursor, item, action, table_name, codigo_counts
        ))

        if field_errors:
            errors.append({"fila": index + 1, "errors": field_errors})
    return errors


def validate_item_fields(item: Dict, action: str, resource_config: Dict) -> Dict[str, str]:
    """Validar campos requeridos según la acción"""
    field_errors = {}
    required_fields = resource_config["required_fields"]
    if action in ['M', 'E']:
        required_fields = {'codigo_identificacion'}

    for field in required_fields:
        value = item.get(field)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            field_errors[field] = f"El campo '{
                field}' es requerido para la acción '{action}'."
    return field_errors


def validate_tipo_identificacion(item: Dict, action: str) -> Dict[str, str]:
    """Validar tipo_identificacion"""
    field_errors = {}
    if action in ['A', 'M'] and 'tipo_identificacion' in item:
        tipo_identificacion = item.get("tipo_identificacion", "")
        if tipo_identificacion:
            if not (len(tipo_identificacion) == 2 and tipo_identificacion.isdigit() and 1 <= int(tipo_identificacion) <= 20):
                field_errors["tipo_identificacion"] = "El campo 'tipo_identificacion' debe ser un string de 2 dígitos numéricos entre 01 y 20."
    return field_errors


def validate_fecha_afiliacion(item: Dict, action: str) -> Dict[str, str]:
    """Validar fecha_afiliacion"""
    field_errors = {}
    if action in ['A', 'M'] and 'fecha_afiliacion' in item:
        fecha_afiliacion = item.get("fecha_afiliacion", "")
        if fecha_afiliacion:
            date_pattern = r'^\d{4}-\d{2}-\d{2}$'
            if not re.match(date_pattern, fecha_afiliacion):
                field_errors["fecha_afiliacion"] = "El campo 'fecha_afiliacion' debe estar en formato YYYY-MM-DD."
    return field_errors


def validate_database_operations(cursor, item: Dict, action: str,
                                 table_name: str, codigo_counts: Dict[str, int]) -> Dict[str, str]:
    """Validar operaciones relacionadas con la base de datos"""
    field_errors = {}

    if action in ['M', 'E'] and 'codigo_identificacion' in item:
        check_sql = f"SELECT COUNT(*) FROM {
            table_name} WHERE codigo_identificacion = %s"
        cursor.execute(check_sql, (item["codigo_identificacion"],))
        if cursor.fetchone()[0] == 0:
            field_errors["codigo_identificacion"] = f"El codigo_identificacion '{
                item['codigo_identificacion']}' no existe en la base de datos para {action}."

    if action == 'A' and 'codigo_identificacion' in item:
        if codigo_counts.get(item["codigo_identificacion"], 0) > 1:
            field_errors["codigo_identificacion"] = f"El codigo_identificacion '{
                item['codigo_identificacion']}' está duplicado en la solicitud."
        else:
            check_sql = f"SELECT COUNT(*) FROM {
                table_name} WHERE codigo_identificacion = %s"
            cursor.execute(check_sql, (item["codigo_identificacion"],))
            if cursor.fetchone()[0] > 0:
                field_errors["codigo_identificacion"] = f"El codigo_identificacion '{
                    item['codigo_identificacion']}' ya existe en la base de datos (use acción M para modificar)."

    return field_errors


def validate_and_process_client_data(cursor, resource_config, data):
    """Función para validar y procesar datos de clientes según acción (A, E, M)"""
    errors = validate_client_data(cursor, resource_config, data)
    processing_results = process_valid_client_data(
        cursor, resource_config, data, errors)

    return {
        "inserted_count": processing_results["inserted"],
        "updated_count": processing_results["updated"],
        "deleted_count": processing_results["deleted"],
        "errors": errors + processing_results["processing_errors"]
    }


def validate_client_data(cursor, resource_config, data):
    """Validar los datos del cliente"""
    errors = []
    table_name = f'"{resource_config["db_schema"]}"."{
        resource_config["db_table"]}"'

    for index, item in enumerate(data):
        action = item.get('accion')
        field_errors = validate_client_item(
            cursor, resource_config, table_name, item, action)

        if field_errors:
            errors.append({
                "fila": index + 1,
                "errors": field_errors
            })

    return errors


def validate_client_item(cursor, resource_config, table_name, item, action):
    """Validar un ítem individual de cliente"""
    field_errors = {}

    # Validar campos requeridos
    required_fields = resource_config["required_fields"]
    if action in ['M', 'E']:
        required_fields = {'codigo_identificacion'}

    for field in required_fields:
        value = item.get(field)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            field_errors[field] = f"El campo '{
                field}' es requerido para la acción '{action}'."

    # Validar existencia en BD para acciones M y E
    if action in ['M', 'E'] and 'codigo_identificacion' in item and not field_errors.get('codigo_identificacion'):
        check_existence_in_db(
            cursor, table_name, item["codigo_identificacion"], field_errors, action)

    return field_errors


def check_existence_in_db(cursor, table_name, codigo_identificacion, field_errors, action):
    """Verificar si un código de identificación existe en la base de datos"""
    check_sql = f"SELECT COUNT(*) FROM {
        table_name} WHERE codigo_identificacion = %s"
    cursor.execute(check_sql, (codigo_identificacion,))
    if cursor.fetchone()[0] == 0:
        field_errors["codigo_identificacion"] = (
            f"El codigo_identificacion '{
                codigo_identificacion}' no existe en la base de datos para {action}."
        )


def process_valid_client_data(cursor, resource_config, data, validation_errors):
    """Procesar los datos válidos del cliente"""
    inserted = 0
    updated = 0
    deleted = 0
    processing_errors = []
    table_name = f'"{resource_config["db_schema"]}"."{
        resource_config["db_table"]}"'

    for index, item in enumerate(data):
        if any(e["fila"] == index + 1 for e in validation_errors):
            continue  # Saltar registros con errores de validación

        action = item.get('accion')
        process_result = process_client_item(
            cursor, resource_config, table_name, item, action, index
        )

        if process_result["success"]:
            if action == 'A':
                inserted += 1
            elif action == 'M':
                updated += 1
            elif action == 'E':
                deleted += 1
        else:
            processing_errors.append(process_result["error"])

    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "processing_errors": processing_errors
    }


def process_client_item(cursor, resource_config, table_name, item, action, index):
    """Procesar un ítem individual de cliente"""
    try:
        if action == 'A':
            return handle_insert(cursor, resource_config, table_name, item)
        elif action == 'M':
            return handle_update(cursor, resource_config, table_name, item, index)
        elif action == 'E':
            return handle_delete(cursor, table_name, item, index)
    except Exception as e:
        return {
            "success": False,
            "error": {
                "fila": index + 1,
                "errors": {"general": f"Error al procesar registro: {str(e)}"}
            }
        }


def handle_insert(cursor, resource_config, table_name, item):
    """Manejar inserción de nuevo cliente"""
    item['id_cliente'] = str(uuid.uuid4())
    item.setdefault('numero_tarjeta', None)
    columns = [col for col in resource_config["columns"]
               if col != "id_parametrizacion_trama"]
    values = [item.get(col) for col in columns]

    columns_sql = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
    cursor.execute(sql, tuple(values))

    return {"success": True}


def handle_update(cursor, resource_config, table_name, item, index):
    """Manejar actualización de cliente existente"""
    set_clauses = []
    values = []
    for col in resource_config["columns"]:
        if col in item and col != "codigo_identificacion":
            set_clauses.append(f'"{col}" = %s')
            values.append(item[col])

    values.append(item["codigo_identificacion"])
    sql = f"UPDATE {table_name} SET {
        ', '.join(set_clauses)} WHERE codigo_identificacion = %s"
    cursor.execute(sql, tuple(values))

    if cursor.rowcount > 0:
        return {"success": True}
    else:
        return {
            "success": False,
            "error": {
                "fila": index + 1,
                "errors": {
                    "general": f"No se pudo actualizar el registro con codigo_identificacion '{item['codigo_identificacion']}'"
                }
            }
        }


def handle_delete(cursor, table_name, item, index):
    """Manejar eliminación de cliente"""
    sql = f"DELETE FROM {table_name} WHERE codigo_identificacion = %s"
    cursor.execute(sql, (item["codigo_identificacion"],))

    if cursor.rowcount > 0:
        return {"success": True}
    else:
        return {
            "success": False,
            "error": {
                "fila": index + 1,
                "errors": {
                    "general": f"No se pudo eliminar el registro con codigo_identificacion '{item['codigo_identificacion']}'"
                }
            }
        }


def get_client_info(cursor, contactabilidad_config, productos_config, id_cliente):
    """
    Obtiene la información de contactabilidad y productos de un cliente por id_cliente.
    Args:
        cursor: Cursor de la base de datos.
        contactabilidad_config: Configuración del recurso contactabilidad desde config.py.
        productos_config: Configuración del recurso productos desde config.py.
        id_cliente: UUID del cliente a consultar.
    Returns:
        dict: Datos combinados de contactabilidad (un registro o null) y productos (lista).
    """
    result = {
        "contactabilidad": None,
        "productos": []
    }

    # Consultar contactabilidad (1:1)
    contactabilidad_table = f'"{contactabilidad_config["db_schema"]}"."{
        contactabilidad_config["db_table"]}"'
    contactabilidad_sql = f"SELECT * FROM {
        contactabilidad_table} WHERE id_cliente = %s"

    try:
        cursor.execute(contactabilidad_sql, (id_cliente,))
        row = cursor.fetchone()
        if row:
            column_names = [desc[0] for desc in cursor.description]
            result["contactabilidad"] = dict(zip(column_names, row))
    except Exception as e:
        logger.error(f"Error al consultar contactabilidad para id_cliente {
                     id_cliente}: {e}")
        raise

    # Consultar productos (1:N)
    productos_table = f'"{productos_config["db_schema"]}"."{
        productos_config["db_table"]}"'
    productos_sql = f"SELECT * FROM {
        productos_table} WHERE id_cliente = %s ORDER BY id"

    try:
        cursor.execute(productos_sql, (id_cliente,))
        rows = cursor.fetchall()
        if rows:
            column_names = [desc[0] for desc in cursor.description]
            result["productos"] = [dict(zip(column_names, row))
                                   for row in rows]
    except Exception as e:
        logger.error(f"Error al consultar productos para id_cliente {
                     id_cliente}: {e}")
        raise

    return result
