import pytest
from unittest import mock
from app.db_connection import conexion_bd
import psycopg2
import os

def test_conexion_bd_success():
    with mock.patch('psycopg2.connect') as mock_connect:
        # Configurar mock para éxito
        mock_conn = mock.MagicMock()
        mock_connect.return_value = mock_conn
        
        # Ejecutar
        conn = conexion_bd()
        
        # Verificar
        assert conn == mock_conn
        mock_connect.assert_called_once_with(
            dbname=os.getenv('NAME_DB_POSTGRES'),
            user=os.getenv('USER_NAME_POSTGRES'),
            password=os.getenv('PASSWORD_POSTGRES'),
            host=os.getenv('HOST_POSTGRES'),
            port=os.getenv('PORT_POSTGRES')
        )

def test_conexion_bd_failure():
    with mock.patch('psycopg2.connect') as mock_connect:
        # Configurar mock para fallar
        mock_connect.side_effect = psycopg2.Error("Connection failed")
        
        # Verificar que se lanza la excepción
        with pytest.raises(psycopg2.Error):
            conexion_bd()
        
