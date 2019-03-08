import json

from google.protobuf import json_format

from ._protos.public.modeldb import CommonService_pb2 as _CommonService


def jsonify(msg):
    return json.loads(json_format.MessageToJson(msg,
                                                preserving_proto_field_name=True,
                                                use_integers_for_enums=True))


def get_proto_type(val):
    if isinstance(val, float) or isinstance(val, int):
        return _CommonService.ValueTypeEnum.NUMBER
    else:
        return _CommonService.ValueTypeEnum.STRING


def cast_to_python(val, proto_type):
    if proto_type == "NUMBER":
        try:
            return int(val)
        except ValueError:
            return float(val)
    else:
        return str(val)
