import re
import ast
import time

import requests

from ._protos.public.modeldb import CommonService_pb2 as _CommonService
from ._protos.public.modeldb import ProjectService_pb2 as _ProjectService
from ._protos.public.modeldb import ExperimentService_pb2 as _ExperimentService
from ._protos.public.modeldb import ExperimentRunService_pb2 as _ExperimentRunService
from . import _utils


class ModelDBClient:
    _GRPC_PREFIX = "Grpc-Metadata-"

    def __init__(self, host="localhost", port="8080", email=None, dev_key=None):
        if email is None and dev_key is None:
            self._auth = None
        elif email is not None and dev_key is not None:
            self._auth = {self._GRPC_PREFIX+'email': email,
                          self._GRPC_PREFIX+'developer_key': dev_key,
                          self._GRPC_PREFIX+'source': "PythonClient"}
        else:
            raise ValueError("`email` and `dev_key` must be provided together")

        self._socket = "{}:{}".format(host, port)

        self.proj = None
        self.expt = None

    @property
    def expt_runs(self):
        if self.expt is None:
            return None
        else:
            Message = _ExperimentRunService.GetExperimentRunsInProject
            msg = Message(project_id=self.proj._id)
            data = _utils.proto_to_json(msg)
            response = requests.get("http://{}/v1/experiment-run/getExperimentRunsInProject".format(self._socket),
                                    params=data, headers=self._auth)
            if response.ok:
                response_msg = _utils.json_to_proto(response.json(), Message.Response)
                expt_run_ids = [expt_run.id
                                for expt_run in response_msg.experiment_runs
                                if expt_run.experiment_id == self.expt._id]
                return ExperimentRuns(self._auth, self._socket, expt_run_ids)
            else:
                raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def set_project(self, proj_name=None, desc=None, tags=None, attrs=None):
        # if proj already in progress, reset expt
        if self.proj is not None:
            self.expt = None

        proj = Project(self._auth, self._socket,
                       proj_name,
                       desc, tags, attrs)

        self.proj = proj
        return proj

    def set_experiment(self, expt_name=None, desc=None, tags=None, attrs=None):
        if self.proj is None:
            raise AttributeError("a project must first in progress")

        expt = Experiment(self._auth, self._socket,
                          self.proj._id, expt_name,
                          desc, tags, attrs)

        self.expt = expt
        return expt

    def set_experiment_run(self, expt_run_name=None, desc=None, tags=None, attrs=None):
        if self.expt is None:
            raise AttributeError("an experiment must first in progress")

        return ExperimentRun(self._auth, self._socket,
                             self.proj._id, self.expt._id, expt_run_name,
                             desc, tags, attrs)


class Project:
    def __init__(self, auth, socket,
                 proj_name=None,
                 desc=None, tags=None, attrs=None,
                 *, _proj_id=None):
        if proj_name is not None and _proj_id is not None:
            raise ValueError("cannot specify both `proj_name` and `_proj_id`")

        if _proj_id is not None:
            proj = Project._get(auth, socket, _proj_id=_proj_id)
            if proj is not None:
                print("set existing Project: {}".format(proj.name))
            else:
                raise ValueError("Project with ID {} not found".format(_proj_id))
        else:
            if proj_name is None:
                proj_name = Project._generate_default_name()
            proj = Project._get(auth, socket, proj_name)
            if proj is not None:
                if any(param is not None for param in (desc, tags, attrs)):
                    raise ValueError("Project with name {} already exists;"
                                     " cannot initialize `desc`, `tags`, or `attrs`".format(proj_name))
                print("set existing Project: {}".format(proj.name))
            else:
                proj = Project._create(auth, socket, proj_name, desc, tags, attrs)
                print("created new Project: {}".format(proj.name))

        self._auth = auth
        self._socket = socket
        self._id = proj.id

    @property
    def name(self):
        Message = _ProjectService.GetProjectById
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/project/getProjectById".format(self._socket),
                                params=data, headers=self._auth)
        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.project.name
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    @staticmethod
    def _generate_default_name():
        return "Project {}".format(str(time.time()).replace('.', ''))

    @staticmethod
    def _get(auth, socket, proj_name=None, *, _proj_id=None):
        if _proj_id is not None:
            Message = _ProjectService.GetProjectById
            msg = Message(id=_proj_id)
            data = _utils.proto_to_json(msg)
            response = requests.get("http://{}/v1/project/getProjectById".format(socket),
                                    params=data, headers=auth)

            if response.ok:
                response_msg = _utils.json_to_proto(response.json(), Message.Response)
                return response_msg.project
            else:
                if response.status_code == 404 and response.json()['code'] == 5:
                    return None
                else:
                    raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))
        elif proj_name is not None:
            Message = _ProjectService.GetProjectByName
            msg = Message(name=proj_name)
            data = _utils.proto_to_json(msg)
            response = requests.get("http://{}/v1/project/getProjectByName".format(socket),
                                    params=data, headers=auth)

            if response.ok:
                response_msg = _utils.json_to_proto(response.json(), Message.Response)
                return response_msg.project_by_user[0]
            else:
                if response.status_code == 404 and response.json()['code'] == 5:
                    return None
                else:
                    raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))
        else:
            raise ValueError("insufficient arguments")

    @staticmethod
    def _create(auth, socket, proj_name, desc=None, tags=None, attrs=None):
        if attrs is not None:
            attrs = [_CommonService.KeyValue(key=key, value=_utils.python_to_val_proto(value))
                     for key, value in attrs.items()]

        Message = _ProjectService.CreateProject
        msg = Message(name=proj_name, description=desc, tags=tags, metadata=attrs)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/project/createProject".format(socket),
                                 json=data, headers=auth)

        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.project
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def find(self, where, ret_all_info=False):
        expt_runs = ExperimentRuns(self._auth, self._socket)
        return expt_runs.find(where, ret_all_info, _proj_id=self._id)

    def top_k(self, key, k, ret_all_info=False):
        expt_runs = ExperimentRuns(self._auth, self._socket)
        return expt_runs.top_k(key, k, ret_all_info, _proj_id=self._id)

    def bottom_k(self, key, k, ret_all_info=False):
        expt_runs = ExperimentRuns(self._auth, self._socket)
        return expt_runs.bottom_k(key, k, ret_all_info, _proj_id=self._id)


class Experiment:
    def __init__(self, auth, socket,
                 proj_id=None, expt_name=None,
                 desc=None, tags=None, attrs=None,
                 *, _expt_id=None):
        if expt_name is not None and _expt_id is not None:
            raise ValueError("cannot specify both `expt_name` and `_expt_id`")

        if _expt_id is not None:
            expt = Experiment._get(auth, socket, _expt_id=_expt_id)
            if expt is not None:
                print("set existing Experiment: {}".format(expt.name))
            else:
                raise ValueError("Experiment with ID {} not found".format(_expt_id))
        elif proj_id is not None:
            if expt_name is None:
                expt_name = Experiment._generate_default_name()
            expt = Experiment._get(auth, socket, proj_id, expt_name)
            if expt is not None:
                if any(param is not None for param in (desc, tags, attrs)):
                    raise ValueError("Experiment with name {} already exists;"
                                     " cannot initialize `desc`, `tags`, or `attrs`".format(expt_name))
                print("set existing Experiment: {}".format(expt.name))
            else:
                expt = Experiment._create(auth, socket, proj_id, expt_name, desc, tags, attrs)
                print("created new Experiment: {}".format(expt.name))
        else:
            raise ValueError("insufficient arguments")

        self._auth = auth
        self._socket = socket
        self._id = expt.id

    @property
    def name(self):
        Message = _ExperimentService.GetExperimentById
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment/getExperimentById".format(self._socket),
                                params=data, headers=self._auth)
        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.experiment.name
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    @staticmethod
    def _generate_default_name():
        return "Experiment {}".format(str(time.time()).replace('.', ''))

    @staticmethod
    def _get(auth, socket, proj_id=None, expt_name=None, *, _expt_id=None):
        if _expt_id is not None:
            Message = _ExperimentService.GetExperimentById
            msg = Message(id=_expt_id)
            data = _utils.proto_to_json(msg)
            response = requests.get("http://{}/v1/experiment/getExperimentById".format(socket),
                                    params=data, headers=auth)
        elif None not in (proj_id, expt_name):
            Message = _ExperimentService.GetExperimentByName
            msg = Message(project_id=proj_id, name=expt_name)
            data = _utils.proto_to_json(msg)
            response = requests.get("http://{}/v1/experiment/getExperimentByName".format(socket),
                                    params=data, headers=auth)
        else:
            raise ValueError("insufficient arguments")

        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.experiment
        else:
            if response.status_code == 404 and response.json()['code'] == 5:
                return None
            else:
                raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    @staticmethod
    def _create(auth, socket, proj_id, expt_name, desc=None, tags=None, attrs=None):
        if attrs is not None:
            attrs = [_CommonService.KeyValue(key=key, value=_utils.python_to_val_proto(value))
                     for key, value in attrs.items()]

        Message = _ExperimentService.CreateExperiment
        msg = Message(project_id=proj_id, name=expt_name,
                      description=desc, tags=tags, attributes=attrs)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment/createExperiment".format(socket),
                                 json=data, headers=auth)

        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.experiment
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def find(self, where, ret_all_info=False):
        expt_runs = ExperimentRuns(self._auth, self._socket)
        return expt_runs.find(where, ret_all_info, _expt_id=self._id)

    def top_k(self, key, k, ret_all_info=False):
        expt_runs = ExperimentRuns(self._auth, self._socket)
        return expt_runs.top_k(key, k, ret_all_info, _expt_id=self._id)

    def bottom_k(self, key, k, ret_all_info=False):
        expt_runs = ExperimentRuns(self._auth, self._socket)
        return expt_runs.bottom_k(key, k, ret_all_info, _expt_id=self._id)


class ExperimentRuns:
    _OP_MAP = {'==': _ExperimentRunService.OperatorEnum.EQ,
               '!=': _ExperimentRunService.OperatorEnum.NE,
               '>':  _ExperimentRunService.OperatorEnum.GT,
               '>=': _ExperimentRunService.OperatorEnum.GTE,
               '<':  _ExperimentRunService.OperatorEnum.LT,
               '<=': _ExperimentRunService.OperatorEnum.LTE}
    _OP_PATTERN = re.compile(r"({})".format('|'.join(sorted(_OP_MAP.keys(), key=lambda s: len(s), reverse=True))))

    def __init__(self, auth, socket, expt_run_ids=None):
        self._auth = auth
        self._socket = socket
        self._ids = expt_run_ids if expt_run_ids is not None else []

    def __repr__(self):
        return "<ExperimentRuns containing {} runs>".format(self.__len__())

    def __getitem__(self, key):
        if isinstance(key, int):
            expt_run_id = self._ids[key]
            return ExperimentRun(self._auth, self._socket, _expt_run_id=expt_run_id)
        elif isinstance(key, slice):
            expt_run_ids = self._ids[key]
            return self.__class__(self._auth, self._socket, expt_run_ids)
        else:
            raise TypeError("index must be integer or slice, not {}".format(type(key)))

    def __len__(self):
        return len(self._ids)

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return self.__class__(self._auth, self._socket, self._ids + other._ids)
        else:
            return NotImplemented

    def find(self, where, ret_all_info=False, *, _proj_id=None, _expt_id=None):
        if _proj_id is not None and _expt_id is not None:
            raise ValueError("cannot specify both `_proj_id` and `_expt_id`")
        elif _proj_id is None and _expt_id is None:
            if self.__len__() == 0:
                return self.__class__(self._auth, self._socket)
            else:
                expt_run_ids = self._ids
        else:
            expt_run_ids = None

        predicates = []
        if isinstance(where, str):
            where = [where]
        for predicate in where:
            # split predicate
            try:
                key, operator, value = map(str.strip, self._OP_PATTERN.split(predicate, maxsplit=1))
            except ValueError:
                raise ValueError("predicate `{}` must be a two-operand comparison".format(predicate))

            # cast operator into protobuf enum variant
            operator = self._OP_MAP[operator]

            # parse value
            try:
                expr_node = ast.parse(value, mode='eval')
            except SyntaxError:
                raise ValueError("value `{}` must be a number or string literal".format(value))
            value_node = expr_node.body
            if type(value_node) is ast.Num:
                value = value_node.n
            elif type(value_node) is ast.Str:
                value = value_node.s
            elif type(value_node) is ast.Compare:
                raise ValueError("predicate `{}` must be a two-operand comparison".format(predicate))
            else:
                raise ValueError("value `{}` must be a number or string literal".format(value))

            predicates.append(_ExperimentRunService.KeyValueQuery(key=key, value=_utils.python_to_val_proto(value),
                                                                  operator=operator))
        Message = _ExperimentRunService.FindExperimentRuns
        msg = Message(project_id=_proj_id, experiment_id=_expt_id, experiment_run_ids=expt_run_ids,
                      predicates=predicates, ids_only=not ret_all_info)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/findExperimentRuns".format(self._socket),
                                 json=data, headers=self._auth)
        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            if ret_all_info:
                return response_msg.experiment_runs
            else:
                return self.__class__(self._auth, self._socket,
                                      [expt_run.id for expt_run in response_msg.experiment_runs])
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def sort(self, key, descending=False, ret_all_info=False):
        if self.__len__() == 0:
            return self.__class__(self._auth, self._socket)

        Message = _ExperimentRunService.SortExperimentRuns
        msg = Message(experiment_run_ids=self._ids,
                      sort_key=key, ascending=not descending, ids_only=not ret_all_info)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/sortExperimentRuns".format(self._socket),
                                params=data, headers=self._auth)
        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            if ret_all_info:
                return response_msg.experiment_runs
            else:
                return self.__class__(self._auth, self._socket,
                                      [expt_run.id for expt_run in response_msg.experiment_runs])
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def top_k(self, key, k, ret_all_info=False, *, _proj_id=None, _expt_id=None):
        if _proj_id is not None and _expt_id is not None:
            raise ValueError("cannot specify both `_proj_id` and `_expt_id`")
        elif _proj_id is None and _expt_id is None:
            if self.__len__() == 0:
                return self.__class__(self._auth, self._socket)
            else:
                expt_run_ids = self._ids
        else:
            expt_run_ids = None

        Message = _ExperimentRunService.TopExperimentRunsSelector
        msg = Message(project_id=_proj_id, experiment_id=_expt_id, experiment_run_ids=expt_run_ids,
                      sort_key=key, ascending=False, top_k=k, ids_only=not ret_all_info)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getTopExperimentRuns".format(self._socket),
                                params=data, headers=self._auth)
        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            if ret_all_info:
                return response_msg.experiment_runs
            else:
                return self.__class__(self._auth, self._socket,
                                      [expt_run.id for expt_run in response_msg.experiment_runs])
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def bottom_k(self, key, k, ret_all_info=False, *, _proj_id=None, _expt_id=None):
        if _proj_id is not None and _expt_id is not None:
            raise ValueError("cannot specify both `_proj_id` and `_expt_id`")
        elif _proj_id is None and _expt_id is None:
            if self.__len__() == 0:
                return self.__class__(self._auth, self._socket)
            else:
                expt_run_ids = self._ids
        else:
            expt_run_ids = None

        Message = _ExperimentRunService.TopExperimentRunsSelector
        msg = Message(project_id=_proj_id, experiment_id=_expt_id, experiment_run_ids=expt_run_ids,
                      sort_key=key, ascending=True, top_k=k, ids_only=not ret_all_info)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getTopExperimentRuns".format(self._socket),
                                params=data, headers=self._auth)
        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            if ret_all_info:
                return response_msg.experiment_runs
            else:
                return self.__class__(self._auth, self._socket,
                                      [expt_run.id for expt_run in response_msg.experiment_runs])
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))


class ExperimentRun:
    def __init__(self, auth, socket,
                 proj_id=None, expt_id=None, expt_run_name=None,
                 desc=None, tags=None, attrs=None,
                 *, _expt_run_id=None):
        if expt_run_name is not None and _expt_run_id is not None:
            raise ValueError("cannot specify both `expt_run_name` and `_expt_run_id`")

        if _expt_run_id is not None:
            expt_run = ExperimentRun._get(auth, socket, _expt_run_id=_expt_run_id)
            if expt_run is not None:
                pass
            else:
                raise ValueError("ExperimentRun with ID {} not found".format(_expt_run_id))
        elif None not in (proj_id, expt_id):
            if expt_run_name is None:
                expt_run_name = ExperimentRun._generate_default_name()
            expt_run = ExperimentRun._get(auth, socket, proj_id, expt_id, expt_run_name)
            if expt_run is not None:
                if any(param is not None for param in (desc, tags, attrs)):
                    raise ValueError("ExperimentRun with name {} already exists;"
                                     " cannot initialize `desc`, `tags`, or `attrs`".format(expt_run_name))
                pass
            else:
                expt_run = ExperimentRun._create(auth, socket, proj_id, expt_id, expt_run_name, desc, tags, attrs)
        else:
            raise ValueError("insufficient arguments")

        self._auth = auth
        self._socket = socket
        self._id = expt_run.id

    @property
    def name(self):
        Message = _ExperimentRunService.GetExperimentRunById
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getExperimentRunById".format(self._socket),
                                params=data, headers=self._auth)
        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.experiment_run.name
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    @staticmethod
    def _generate_default_name():
        return "ExperimentRun {}".format(str(time.time()).replace('.', ''))

    @staticmethod
    def _get(auth, socket, proj_id=None, expt_id=None, expt_run_name=None, *, _expt_run_id=None):
        if _expt_run_id is not None:
            Message = _ExperimentRunService.GetExperimentRunById
            msg = Message(id=_expt_run_id)
            data = _utils.proto_to_json(msg)
            response = requests.get("http://{}/v1/experiment-run/getExperimentRunById".format(socket),
                                    params=data, headers=auth)
        elif None not in (proj_id, expt_id, expt_run_name):
            Message = _ExperimentRunService.GetExperimentRunsInProject
            msg = Message(project_id=proj_id)
            data = _utils.proto_to_json(msg)
            response = requests.get("http://{}/v1/experiment-run/getExperimentRunsInProject".format(socket),
                                    params=data, headers=auth)
            if not response.ok:
                raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))
            else:
                if 'experiment_runs' in response.json():
                    response_msg = _utils.json_to_proto(response.json(), Message.Response)
                    result = [expt_run
                              for expt_run in response_msg.experiment_runs
                              if expt_run.name == expt_run_name]
                    return result[-1] if len(result) else None
                else:  # no expt_runs in proj
                    return None
        else:
            raise ValueError("insufficient arguments")

        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.experiment_run
        else:
            if response.status_code == 404 and response.json()['code'] == 5:
                return None
            else:
                raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    @staticmethod
    def _create(auth, socket, proj_id, expt_id, expt_run_name, desc=None, tags=None, attrs=None):
        if attrs is not None:
            attrs = [_CommonService.KeyValue(key=key, value=_utils.python_to_val_proto(value))
                     for key, value in attrs.items()]

        Message = _ExperimentRunService.CreateExperimentRun
        msg = Message(project_id=proj_id, experiment_id=expt_id, name=expt_run_name,
                      description=desc, tags=tags, attributes=attrs)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/createExperimentRun".format(socket),
                                 json=data, headers=auth)

        if response.ok:
            response_msg = _utils.json_to_proto(response.json(), Message.Response)
            return response_msg.experiment_run
        else:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def log_attribute(self, name, value):
        attribute = _CommonService.KeyValue(key=name, value=_utils.python_to_val_proto(value))
        msg = _ExperimentRunService.LogAttribute(id=self._id, attribute=attribute)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/logAttribute".format(self._socket),
                                 json=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def get_attribute(self, name):
        Message = _ExperimentRunService.GetAttributes
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getAttributes".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {attribute.key: _utils.val_proto_to_python(attribute.value)
                for attribute in response_msg.attributes}[name]

    def get_attributes(self):
        Message = _ExperimentRunService.GetAttributes
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getAttributes".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {attribute.key: _utils.val_proto_to_python(attribute.value)
                for attribute in response_msg.attributes}

    def log_metric(self, name, value):
        metric = _CommonService.KeyValue(key=name, value=_utils.python_to_val_proto(value))
        msg = _ExperimentRunService.LogMetric(id=self._id, metric=metric)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/logMetric".format(self._socket),
                                 json=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def get_metric(self, name):
        Message = _ExperimentRunService.GetMetrics
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getMetrics".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {metric.key: _utils.val_proto_to_python(metric.value)
                for metric in response_msg.metrics}[name]

    def get_metrics(self):
        Message = _ExperimentRunService.GetMetrics
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getMetrics".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {metric.key: _utils.val_proto_to_python(metric.value)
                for metric in response_msg.metrics}

    def log_hyperparameter(self, name, value):
        hyperparameter = _CommonService.KeyValue(key=name, value=_utils.python_to_val_proto(value))
        msg = _ExperimentRunService.LogHyperparameter(id=self._id, hyperparameter=hyperparameter)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/logHyperparameter".format(self._socket),
                                 json=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def get_hyperparameter(self, name):
        Message = _ExperimentRunService.GetHyperparameters
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getHyperparameters".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {hyperparameter.key: _utils.val_proto_to_python(hyperparameter.value)
                for hyperparameter in response_msg.hyperparameters}[name]

    def get_hyperparameters(self):
        Message = _ExperimentRunService.GetHyperparameters
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getHyperparameters".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {hyperparameter.key: _utils.val_proto_to_python(hyperparameter.value)
                for hyperparameter in response_msg.hyperparameters}

    def log_dataset(self, name, path):
        dataset = _CommonService.Artifact(key=name, path=path,
                                          artifact_type=_CommonService.ArtifactTypeEnum.DATA)
        msg = _ExperimentRunService.LogDataset(id=self._id, dataset=dataset)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/logDataset".format(self._socket),
                                 json=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def get_dataset(self, name):
        Message = _ExperimentRunService.GetDatasets
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getDatasets".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {dataset.key: dataset.path for dataset in response_msg.datasets}[name]

    def get_datasets(self):
        Message = _ExperimentRunService.GetDatasets
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getDatasets".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {dataset.key: dataset.path for dataset in response_msg.datasets}

    def log_model(self, name, path):
        model = _CommonService.Artifact(key=name, path=path,
                                        artifact_type=_CommonService.ArtifactTypeEnum.MODEL)
        msg = _ExperimentRunService.LogArtifact(id=self._id, artifact=model)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/logArtifact".format(self._socket),
                                 json=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def get_model(self, name):
        Message = _ExperimentRunService.GetArtifacts
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getArtifacts".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {artifact.key: artifact.path
                for artifact in response_msg.artifacts
                if artifact.artifact_type == _CommonService.ArtifactTypeEnum.MODEL}[name]

    def get_models(self):
        Message = _ExperimentRunService.GetArtifacts
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getArtifacts".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return {artifact.key: artifact.path
                for artifact in response_msg.artifacts
                if artifact.artifact_type == _CommonService.ArtifactTypeEnum.MODEL}

    def log_image(self, name, path):
        image = _CommonService.Artifact(key=name, path=path,
                                        artifact_type=_CommonService.ArtifactTypeEnum.IMAGE)
        msg = _ExperimentRunService.LogArtifact(id=self._id, artifact=image)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/logArtifact".format(self._socket),
                                 json=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def get_image(self, name):
        Message = _ExperimentRunService.GetArtifacts
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getArtifacts".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return [artifact.path
                for artifact in response_msg.artifacts
                if artifact.artifact_type == _CommonService.ArtifactTypeEnum.IMAGE
                and artifact.key == name][0]

    def get_images(self):
        Message = _ExperimentRunService.GetArtifacts
        msg = Message(id=self._id)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getArtifacts".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return [artifact.path
                for artifact in response_msg.artifacts
                if artifact.artifact_type == _CommonService.ArtifactTypeEnum.IMAGE]

    def log_observation(self, name, value):
        attribute = _CommonService.KeyValue(key=name, value=_utils.python_to_val_proto(value))
        observation = _ExperimentRunService.Observation(attribute=attribute)  # TODO: support Artifacts
        msg = _ExperimentRunService.LogObservation(id=self._id, observation=observation)
        data = _utils.proto_to_json(msg)
        response = requests.post("http://{}/v1/experiment-run/logObservation".format(self._socket),
                                 json=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

    def get_observations(self, name):
        Message = _ExperimentRunService.GetObservations
        msg = Message(id=self._id, observation_key=name)
        data = _utils.proto_to_json(msg)
        response = requests.get("http://{}/v1/experiment-run/getObservations".format(self._socket),
                                params=data, headers=self._auth)
        if not response.ok:
            raise requests.HTTPError("{}: {}".format(response.status_code, response.reason))

        response_msg = _utils.json_to_proto(response.json(), Message.Response)
        return [_utils.val_proto_to_python(observation.attribute.value)
                for observation in response_msg.observations]  # TODO: support Artifacts
