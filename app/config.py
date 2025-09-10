import os

# Configuración de recursos
RESOURCES = {
    "clientes": {
        "db_schema": os.getenv('DB_SCHEMA', 'mstx'),
        "db_table": os.getenv('DB_TABLE', 'clientes'),
        "limit": 10,
        "search_fields": [
            "primer_nombre", "primer_apellido", "numero_identificacion",
            "numero_tarjeta", "numero_celular", "correo_electronico", "segundo_apellido"
        ],
        "columns": [
            "id_cliente", "primer_nombre", "segundo_nombre", "primer_apellido",
            "segundo_apellido", "tipo_identificacion", "numero_identificacion",
            "numero_celular", "correo_electronico", "fecha_afiliacion",
            "codigo_identificacion"
        ],
        "required_fields": {
            "primer_nombre", "primer_apellido", "tipo_identificacion",
            "numero_identificacion", "fecha_afiliacion", "codigo_identificacion"
        }
    },
    "parametrizacion": {
        "db_schema": os.getenv('DB_SCHEMA', 'mstx'),
        "db_table": "parametrizacion_tramas",
        "limit": 10,
        "search_fields": [
            "campo", "descripcion", "tipo", "posicion", "fuente"
        ],
        "columns": [
            "id_parametrizacion_trama", "campo", "descripcion", "tipo",
            "longitud", "posicion", "id_fuente", "fuente", "requerido"
        ],
        "required_fields": {
            "campo", "descripcion", "tipo", "longitud", "posicion", "id_fuente", "fuente", "requerido"
        },
        "valid_options": {"POSTILLION", "STRATUS", "Fiduciaria"},
        "valid_types": {"Numerico", "Alfanumerico"},
        "fuente_mapping": {
            1: "STRATUS",
            2: "FIDUCIARIA",
            3: "POSTILLION"
       }
    },
    "parametrizacion_general": {
        "db_schema": os.getenv('DB_SCHEMA', 'mstx'),
        "db_table": "parametrizacion_general",
        "limit": 10,
        "search_fields": [
            "nombre_campo", "descripcion", "valor"
        ],
        "columns": [
            "nombre_campo", "descripcion",  "valor"
        ],
        "required_fields": {
            "nombre_campo", "descripcion", "valor"
        }
    },
    "contactabilidad": {
        "db_schema": os.getenv('DB_SCHEMA', 'mstx'),
        "db_table": "contactabilidad",
        "columns": [
            "id", "id_cliente", "tipo_notificaciones", "numero_telefonico",
            "numero_whatsapp", "correo_electronico"
        ]
    },
    "productos": {
        "db_schema": os.getenv('DB_SCHEMA', 'mstx'),
        "db_table": "productos",
        "columns": [
            "id", "id_cliente", "tipo_producto", "tipo_servicio", "numero_tarjeta"
        ]
    },
    "historial_transacciones": {
        "db_schema": os.getenv('DB_SCHEMA', 'mstx'),
        "db_table": "historial_transacciones",
        "limit": 10,
        "search_fields": [
            "numero_tarjeta", "fecha", "hora", "estado_notificacion",
            "nombre_comercial", "metodo_notificacion", "mensaje_notificacion"
        ],
        "columns": [
            "id", "id_cliente", "codigo_respuesta", "numero_tarjeta",
            "fecha", "hora", "hora_notificacion", "estado_notificacion",
            "nombre_comercial", "metodo_notificacion", "mensaje_notificacion"
        ]
    }
}

# Configuración de permisos por rol
ROLE_PERMISSIONS = {
    "administrador": ["clientes", "parametrizacion", "parametrizacion_general","informacionCliente", "historial_transaccion", "notificacion_masiva"],
    "usuarios": ["clientes", "informacionCliente", "historial_transaccion"],
    "consultores": ["clientes", "historial_transaccion"],
    "masivos": ["notificacion_masiva"]
}
