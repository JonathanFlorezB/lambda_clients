import os
import psycopg2
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def conexion_bd():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('NAME_DB_POSTGRES'),
            user=os.getenv('USER_NAME_POSTGRES'),
            password=os.getenv('PASSWORD_POSTGRES'),
            host=os.getenv('HOST_POSTGRES'),
            port=os.getenv('PORT_POSTGRES')
        )
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        raise

