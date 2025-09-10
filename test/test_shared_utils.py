import pytest
from app.shared_utils import (
    build_response
)
import json
import patch
import jwt

@pytest.fixture
def mock_role_permissions(monkeypatch):
    mock_permissions = {
        "administrador": ["clientes", "parametrizacion"],
        "consultor": ["clientes"],
        "masivo": [],
        "usuario": []
    }
    monkeypatch.setattr("app.shared_utils.ROLE_PERMISSIONS", mock_permissions)

def test_build_response():
    response = build_response(200, {"test": "value"})
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"test": "value"}
    assert "Access-Control-Allow-Methods" in response["headers"]


def test_build_response_with_none():
    response = build_response(200, None)
    assert json.loads(response["body"]) == None
