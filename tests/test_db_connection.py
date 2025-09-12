import os
from unittest.mock import patch, MagicMock
import pytest
from app.db_connection import conexion_bd

@patch('app.db_connection.psycopg2')
def test_conexion_bd_success(mock_psycopg2):
    """
    Test that conexion_bd calls psycopg2.connect with the correct credentials
    from environment variables and returns a connection object.
    """
    # Arrange
    mock_conn = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn

    # Act
    conn = conexion_bd()

    # Assert
    mock_psycopg2.connect.assert_called_once_with(
        dbname=os.getenv('NAME_DB_POSTGRES'),
        user=os.getenv('USER_NAME_POSTGRES'),
        password=os.getenv('PASSWORD_POSTGRES'),
        host=os.getenv('HOST_POSTGRES'),
        port=os.getenv('PORT_POSTGRES')
    )
    assert conn == mock_conn

@patch('app.db_connection.psycopg2.connect')
@patch('app.db_connection.logger')
def test_conexion_bd_failure(mock_logger, mock_connect):
    """
    Test that conexion_bd logs an error and raises an exception when
    psycopg2.connect fails.
    """
    # Arrange
    test_exception = Exception("Connection failed")
    mock_connect.side_effect = test_exception

    # Act & Assert
    with pytest.raises(Exception) as excinfo:
        conexion_bd()

    assert str(excinfo.value) == "Connection failed"
    mock_logger.error.assert_called_once_with(f"Error al conectar a la base de datos: {test_exception}")
