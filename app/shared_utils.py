import json

def build_response(status_code, body_dict):
    """Devuelve una respuesta con headers CORS estándar"""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT,DELETE",
            #"Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body_dict, default=str)
    }
