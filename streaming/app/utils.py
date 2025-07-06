import json
from datetime import datetime

def serialize_message(message):
    def default_serializer(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
    return json.dumps(message, default=default_serializer).encode('utf-8')

def deserialize_message(message_bytes):
    return json.loads(message_bytes.decode('utf-8'))