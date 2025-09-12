import json
from app.shared_utils import build_response

def test_build_response():
    """
    Test that build_response returns a correctly formatted response dictionary.
    """
    status_code = 200
    body_dict = {"message": "Success"}

    response = build_response(status_code, body_dict)

    assert response["statusCode"] == status_code
    assert "headers" in response
    assert response["headers"]["Access-Control-Allow-Headers"] == "Content-Type,Authorization"
    assert response["headers"]["Access-Control-Allow-Methods"] == "OPTIONS,GET,POST,PUT,DELETE"
    #assert response["headers"]["Access-Control-Allow-Origin"] == "*"

    assert "body" in response
    assert json.loads(response["body"]) == body_dict

def test_build_response_with_complex_body():
    """
    Test build_response with a more complex body to ensure proper JSON serialization.
    """
    status_code = 400
    body_dict = {"errors": ["Invalid input", "Missing field"], "code": 123}

    response = build_response(status_code, body_dict)

    assert response["statusCode"] == status_code
    assert json.loads(response["body"]) == body_dict

def test_build_response_empty_body():
    """
    Test build_response with an empty dictionary as the body.
    """
    status_code = 204
    body_dict = {}

    response = build_response(status_code, body_dict)

    assert response["statusCode"] == status_code
    assert json.loads(response["body"]) == body_dict
