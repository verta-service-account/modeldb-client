import json

from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value, NULL_VALUE


def jsonify(msg):
    return json.loads(json_format.MessageToJson(msg,
                                                preserving_proto_field_name=True,
                                                use_integers_for_enums=True))


def msgify(response_json, msg):
    return json_format.Parse(json.dumps(response_json), msg)


def to_msg(val):
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


def to_python(val):
    if val.HasField("null_value"):
        return None
    if val.HasField("bool_value"):
        return val.bool_value
    if val.HasField("number_value"):
        number_value = val.number_value
        if number_value.is_integer():
            return int(number_value)
        else:
            return number_value
    if val.HasField("string_value"):
        return val.string_value
    if val.HasField("struct_value"):
        raise NotImplementedError()
    if val.HasField("list_value"):
        raise NotImplementedError()
    else:
        raise ValueError("Value is empty")
