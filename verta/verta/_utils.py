import json

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value, NULL_VALUE


def proto_to_json(msg):
    return json.loads(json_format.MessageToJson(msg,
                                                preserving_proto_field_name=True,
                                                use_integers_for_enums=True))


def json_to_proto(response_json, msg):
    return json_format.Parse(json.dumps(response_json), msg)


def python_to_proto(val):
    if val is None:
        return Value(null_value=NULL_VALUE)
    if isinstance(val, bool):  # did you know that `bool` is a subclass of `int`?
        return Value(bool_value=val)
    elif isinstance(val, float) or isinstance(val, int):
        return Value(number_value=val)
    elif isinstance(val, str):
        return Value(string_value=val)
    elif isinstance(val, dict):
        raise NotImplementedError()
    elif isinstance(val, list):
        raise NotImplementedError()
    else:
        raise ValueError("unsupported type {}".format(type(val)))


def proto_to_python(val_msg):
    if val_msg.HasField("null_value"):
        return None
    if val_msg.HasField("bool_value"):
        return val_msg.bool_value
    if val_msg.HasField("number_value"):
        number_value = val_msg.number_value
        if number_value.is_integer():
            return int(number_value)
        else:
            return number_value
    if val_msg.HasField("string_value"):
        return val_msg.string_value
    if val_msg.HasField("struct_value"):
        raise NotImplementedError()
    if val_msg.HasField("list_value"):
        raise NotImplementedError()
    else:
        raise ValueError("Value is empty")
