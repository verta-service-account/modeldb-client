from typing import Any, Union, Tuple

import ast
import time
import json

from google.protobuf import json_format
import requests

from .protos.modeldb import CommonService_pb2
from .protos.modeldb import ExperimentService_pb2
from .protos.modeldb import ProjectService_pb2
from .protos.modeldb import ExperimentRunService_pb2


class ModelDBClient:
    GRPC_PREFIX = "Grpc-Metadata-"
    SOURCE = "PythonClient"

    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = "8080"

    def __init__(self, email, dev_key, source=SOURCE, host=DEFAULT_HOST, port=DEFAULT_PORT):
        self.auth = {f'{self.GRPC_PREFIX}email': email,
                     f'{self.GRPC_PREFIX}developer_key': dev_key,
                     f'{self.GRPC_PREFIX}source': source}

        self.socket = f"{host}:{port}"

        self.proj = None
        self.expt = None

    @property
    def expt_runs(self):
        if self.expt is None:
            return None
        else:
            msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=self.proj.id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.get(f"http://{self.socket}/v1/experiment-run/getExperimentRunsInProject",
                                    params=data, headers=self.auth)
            if response.ok:
                expt_run_ids = [expt_run['id']
                                for expt_run in response.json()['experiment_runs']
                                if expt_run['experiment_id'] == self.expt.id]
                return ExperimentRuns(self.auth, self.socket, expt_run_ids)
            else:
                raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def set_project(self, proj_name=None):
        # TODO: handle case when project is already in progress

        proj = Project(self.auth, self.socket, proj_name)

        self.proj = proj
        return proj

    def set_experiment(self, expt_name=None):
        # TODO: handle case when experiment is already in progress
        if self.proj is None:
            raise AttributeError("a project must first in progress")

        expt = Experiment(self.auth, self.socket, self.proj.id, expt_name)

        self.expt = expt
        return expt

    def set_experiment_run(self, expt_run_name=None):
        if self.expt is None:
            raise AttributeError("an experiment must first in progress")

        return ExperimentRun(self.auth, self.socket,
                             self.proj.id, self.expt.id,
                             expt_run_name)


class Project:
    def __init__(self, auth, socket, proj_name=None, *, proj_id=None):
        if proj_name is not None and proj_id is not None:
            raise ValueError("cannot specify both `proj_name` and `proj_id`")

        if proj_id is not None:
            proj = Project._get(auth, socket, proj_id=proj_id)
            if proj is not None:
                pass
            else:
                raise ValueError(f"Project with ID {proj_id} not found")
        else:
            if proj_name is None:
                proj_name = Project._generate_default_name()
            proj = Project._get(auth, socket, proj_name)
            if proj is not None:
                pass
            else:
                proj = Project._create(auth, socket, proj_name)

        self.auth = auth
        self.socket = socket
        self.id = proj['id']

    @property
    def name(self):
        msg = ProjectService_pb2.GetProjectById(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/project/getProjectById",
                                params=data, headers=self.auth)
        if response.ok:
            return response.json()['name']
        else:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    @staticmethod
    def _generate_default_name():
        return "Project {}".format(int(time.time()))

    @staticmethod
    def _get(auth, socket, proj_name=None, *, proj_id=None):
        if proj_id is not None:
            msg = ProjectService_pb2.GetProjectById(id=proj_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.get(f"http://{socket}/v1/project/getProjectById",
                                    params=data, headers=auth)
        elif proj_name is not None:
            msg = ProjectService_pb2.GetProjectByName(name=proj_name)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.get(f"http://{socket}/v1/project/getProjectByName",
                                    params=data, headers=auth)
        else:
            raise ValueError("insufficient arguments")

        if response.ok:
            return response.json()['project']
        else:
            if ((response.status_code == 401 and response.json()['code'] == 16)
                    or (response.status_code == 404 and response.json()['code'] == 5)):
                return None
            else:
                raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    @staticmethod
    def _create(auth, socket, proj_name):
        msg = ProjectService_pb2.CreateProject(name=proj_name)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{socket}/v1/project/createProject",
                                 json=data, headers=auth)

        if response.ok:
            return response.json()['project']
        else:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")


class Experiment:
    def __init__(self, auth, socket, proj_id=None, expt_name=None, *, expt_id=None):
        if expt_name is not None and expt_id is not None:
            raise ValueError("cannot specify both `expt_name` and `expt_id`")

        if expt_id is not None:
            expt = Experiment._get(auth, socket, expt_id=expt_id)
            if expt is not None:
                pass
            else:
                raise ValueError(f"Experiment with ID {expt_id} not found")
        elif proj_id is not None:
            if expt_name is None:
                expt_name = Experiment._generate_default_name()
            expt = Experiment._get(auth, socket, proj_id, expt_name)
            if expt is not None:
                pass
            else:
                expt = Experiment._create(auth, socket, proj_id, expt_name)
        else:
            raise ValueError("insufficient arguments")

        self.auth = auth
        self.socket = socket
        self.id = expt['id']

    @property
    def name(self):
        msg = ExperimentService_pb2.GetExperimentById(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment/getExperimentById",
                                params=data, headers=self.auth)
        if response.ok:
            return response.json()['name']
        else:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    @staticmethod
    def _generate_default_name():
        return "Experiment {}".format(int(time.time()))

    @staticmethod
    def _get(auth, socket, proj_id=None, expt_name=None, *, expt_id=None):
        if expt_id is not None:
            msg = ExperimentService_pb2.GetExperimentById(id=expt_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.get(f"http://{socket}/v1/experiment/getExperimentById",
                                    params=data, headers=auth)
        elif None not in (proj_id, expt_name):
            msg = ExperimentService_pb2.GetExperimentByName(project_id=proj_id, name=expt_name)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.get(f"http://{socket}/v1/experiment/getExperimentByName",
                                    params=data, headers=auth)
        else:
            raise ValueError("insufficient arguments")

        if response.ok:
            return response.json()['experiment']
        else:
            if ((response.status_code == 401 and response.json()['code'] == 16)
                    or (response.status_code == 404 and response.json()['code'] == 5)):
                return None
            else:
                raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    @staticmethod
    def _create(auth, socket, proj_id, expt_name):
        msg = ExperimentService_pb2.CreateExperiment(project_id=proj_id, name=expt_name)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{socket}/v1/experiment/createExperiment",
                                 json=data, headers=auth)

        if response.ok:
            return response.json()['experiment']
        else:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")


class ExperimentRuns:
    def __init__(self, auth, socket, expt_run_ids=None):
        self.auth = auth
        self.socket = socket
        self.expt_run_ids = expt_run_ids if expt_run_ids is not None else []

    def __getitem__(self, key):
        if isinstance(key, int):
            expt_run_id = self.expt_run_ids[key]
            return ExperimentRun(self.auth, self.socket, expt_run_id=expt_run_id)
        elif isinstance(key, slice):
            expt_run_ids = self.expt_run_ids[key]
            return self.__class__(self.auth, self.socket, expt_run_ids)
        else:
            raise TypeError(f"index must be integer or slice, not {type(key)}")

    def __len__(self):
        return len(self.expt_run_ids)

    def find(self, where, ret_all_info=False, *, proj_name=None, expt_name=None):
        if expt_name is not None and proj_name is None:
            raise ValueError("`proj_name` must also be specified if using `expt_name`")

        # get project id
        if proj_name is not None:
            proj = Project._get(self.auth, self.socket, proj_name)
            if proj is not None:
                proj_id = proj['id']
            else:
                raise ValueError(f"Project with name {proj_name} not found")
        # get experiment id
        if expt_name is not None:
            expt = Experiment._get(self.auth, self.socket, proj_id, expt_name)
            if expt is not None:
                expt_id = expt['id']
            else:
                raise ValueError(f"Experiment with name {expt_name} not found")
        # get experiment run ids
        if proj_name is None and expt_name is None and len(self.expt_run_ids) > 0:
            expt_run_ids = self.expt_run_ids

        predicates = []
        for predicate in where:
            # check that predicate is an expression
            try:
                expr_node = ast.parse(predicate, mode='eval')
            except SyntaxError:
                raise ValueError(f"predicate `{predicate}` must be a valid expression")

            # check that predicate is a comparison between two operands
            comp_node = expr_node.body
            if (not type(comp_node) is ast.Compare
                    or len(comp_node.ops) > 1
                    or len(comp_node.comparators) > 1):
                raise ValueError(f"predicate `{predicate}` must be a two-operand comparison")

            # accumulate key and check for dot notation
            key_node = comp_node.left
            tokens = []
            while type(key_node) is ast.Attribute:
                tokens.append(key_node.attr)
                key_node = key_node.value
            if type(key_node) is ast.Name:
                tokens.append(key_node.id)
            else:
                raise ValueError(f"key of predicate `{predicate}` must be dot notation")
            key = '.'.join(reversed(tokens))

            # cast operator into protobuf enum variant and check for validity
            op_node = comp_node.ops[0]
            operator = _get_proto_op(op_node)

            # get value and its protobuf type variant and check for valitity
            val_node = comp_node.comparators[0]
            if type(val_node) is ast.Num:
                value = val_node.n
            elif type(val_node) is ast.Str:
                value = val_node.s
            else:
                raise ValueError(f"value `{predicate[val_node.col_offset:]}` must be a number or string literal")
            proto_type = _get_proto_type(value)

            print(f"{key} | {operator} | {value} | {proto_type}")
            # TODO: construct message, append to predicates
        # TODO: make call, build new ExptRuns

    def sort(self, by, descending=False, ret_all_info=False):
        raise NotImplementedError()
        # TODO: construct message
        # TODO: make call, built new ExptRuns


class ExperimentRun:
    def __init__(self, auth, socket, proj_id=None, expt_id=None, expt_run_name=None, *, expt_run_id=None):
        if expt_run_name is not None and expt_run_id is not None:
            raise ValueError("cannot specify both `expt_run_name` and `expt_run_id`")

        if expt_run_id is not None:
            expt_run = ExperimentRun._get(auth, socket, expt_run_id=expt_run_id)
            if expt_run is not None:
                pass
            else:
                raise ValueError(f"ExperimentRun with ID {expt_run_id} not found")
        elif None not in (proj_id, expt_id):
            if expt_run_name is None:
                expt_run_name = ExperimentRun._generate_default_name()
            expt_run = ExperimentRun._get(auth, socket, proj_id, expt_id, expt_run_name)
            if expt_run is not None:
                pass
            else:
                expt_run = ExperimentRun._create(auth, socket, proj_id, expt_id, expt_run_name)
        else:
            raise ValueError("insufficient arguments")

        self.auth = auth
        self.socket = socket
        self.id = expt_run['id']

    @property
    def name(self):
        msg = ExperimentRunService_pb2.GetExperimentRunById(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getExperimentRunById",
                                params=data, headers=self.auth)
        if response.ok:
            return response.json()['name']
        else:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    @staticmethod
    def _generate_default_name():
        return "ExperimentRun {}".format(int(time.time()))

    @staticmethod
    def _get(auth, socket, proj_id=None, expt_id=None, expt_run_name=None, *, expt_run_id=None):
        if expt_run_id is not None:
            msg = ExperimentRunService_pb2.GetExperimentRunById(id=expt_run_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.get(f"http://{socket}/v1/experiment-run/getExperimentRunById",
                                    params=data, headers=auth)
        elif None not in (proj_id, expt_id, expt_run_name):
            # TODO: swap blocks when RPC is implemented
            # msg = ExperimentRunService_pb2.GetExperimentByName(project_id=proj_id, experiment_id=expt_id, name=expt_name)
            # data = json.loads(json_format.MessageToJson(msg))
            # response = requests.post(f"http://{socket}/v1/experiment-run/getExperimentRunByName",
            #                          json=data, headers=self.auth)
            msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=proj_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.get(f"http://{socket}/v1/experiment-run/getExperimentRunsInProject",
                                    params=data, headers=auth)
            if not response.ok:
                raise requests.HTTPError(f"{response.status_code}: {response.reason}")
            else:
                if 'experiment_runs' in response.json():
                    result = [expt_run for expt_run in response.json()['experiment_runs'] if expt_run['name'] == expt_run_name]
                    return result[-1] if len(result) else None
                else:  # no expt_runs in proj
                    return None
        else:
            raise ValueError("insufficient arguments")

        if response.ok:
            return response.json()['experiment_run']
        else:
            if ((response.status_code == 401 and response.json()['code'] == 16)
                    or (response.status_code == 404 and response.json()['code'] == 5)):
                return None
            else:
                raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    @staticmethod
    def _create(auth, socket, proj_id, expt_id, expt_run_name):
        msg = ExperimentRunService_pb2.CreateExperimentRun(project_id=proj_id, experiment_id=expt_id, name=expt_run_name)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{socket}/v1/experiment-run/createExperimentRun",
                                 json=data, headers=auth)

        if response.ok:
            return response.json()['experiment_run']
        else:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def log_attribute(self, name, value):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        msg = ExperimentRunService_pb2.LogAttribute(id=self.id,
                                                    attribute=attribute)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/experiment-run/logAttribute",
                                 json=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def get_attributes(self):
        msg = ExperimentRunService_pb2.GetAttributes(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getAttributes",
                                params=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

        return {attribute['key']: _cast_to_python(attribute['value'], attribute.get('value_type', "STRING"))
                for attribute in response.json()['attributes']}

    def log_metric(self, name, value):
        proto_type = _get_proto_type(value)
        metric = CommonService_pb2.KeyValue(key=name, value=str(value),
                                            value_type=proto_type)
        msg = ExperimentRunService_pb2.LogMetric(id=self.id,
                                                 metric=metric)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/experiment-run/logMetric",
                                 json=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def get_metrics(self):
        msg = ExperimentRunService_pb2.GetMetrics(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getMetrics",
                                params=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

        return {metric['key']: _cast_to_python(metric['value'], metric.get('value_type', "STRING"))
                for metric in response.json()['metrics']}

    def log_hyperparameter(self, name, value):
        proto_type = _get_proto_type(value)
        hyperparameter = CommonService_pb2.KeyValue(key=name, value=str(value),
                                                    value_type=proto_type)
        msg = ExperimentRunService_pb2.LogHyperparameter(id=self.id,
                                                         hyperparameter=hyperparameter)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/experiment-run/logHyperparameter",
                                 json=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def get_hyperparameters(self):
        msg = ExperimentRunService_pb2.GetHyperparameters(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getHyperparameters",
                                params=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

        return {hyperparameter['key']: _cast_to_python(hyperparameter['value'], hyperparameter.get('value_type', "STRING"))
                for hyperparameter in response.json()['hyperparameters']}

    def log_dataset(self, name, path):
        dataset = CommonService_pb2.Artifact(key=name, path=path,
                                             artifact_type=CommonService_pb2.ArtifactTypeEnum.DATA)
        msg = ExperimentRunService_pb2.LogDataset(id=self.id,
                                                  dataset=dataset)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/experiment-run/logDataset",
                                 json=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def get_datasets(self):
        msg = ExperimentRunService_pb2.GetDatasets(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getDatasets",
                                params=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

        return {dataset['key']: dataset['path'] for dataset in response.json()['datasets']}

    def log_model(self, name, path):
        model = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.MODEL)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=model)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/experiment-run/logArtifact",
                                 json=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def get_models(self):
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getArtifacts",
                                params=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

        return {artifact['key']: artifact['path']
                for artifact in response.json()['artifacts']
                if artifact.get('artifact_type', "IMAGE") == "MODEL"}

    def log_image(self, name, path):
        image = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.IMAGE)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=image)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/experiment-run/logArtifact",
                                 json=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def get_image(self, name):  # TODO: this, but better
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getArtifacts",
                                params=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

        return [artifact['path']
                for artifact in response.json()['artifacts']
                if artifact.get('artifact_type', "IMAGE") == "IMAGE"
                and artifact['key'] == name][0]

    def log_observation(self, name, value):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        observation = ExperimentRunService_pb2.Observation(attribute=attribute)  # TODO: support Artifacts
        msg = ExperimentRunService_pb2.LogObservation(id=self.id,
                                                      observation=observation)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/experiment-run/logObservation",
                                 json=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

    def get_observations(self, name):
        msg = ExperimentRunService_pb2.GetObservations(id=self.id,
                                                       observation_key=name)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.get(f"http://{self.socket}/v1/experiment-run/getObservations",
                                params=data, headers=self.auth)
        if not response.ok:
            raise requests.HTTPError(f"{response.status_code}: {response.reason}")

        return [_cast_to_python(observation['attribute']['value'], observation['attribute'].get('value_type', "STRING"))
                for observation in response.json()['observations']]  # TODO: support Artifacts


def _get_proto_type(val):
    if isinstance(val, float) or isinstance(val, int):
        return CommonService_pb2.ValueTypeEnum.NUMBER
    else:
        return CommonService_pb2.ValueTypeEnum.STRING


def _cast_to_python(val, proto_type):
    if proto_type == "NUMBER":
        try:
            return int(val)
        except ValueError:
            return float(val)
    else:
        return str(val)


def _get_proto_op(op_node):  # TODO: use proto types
    op_type = type(op_node)
    if op_type is ast.Eq:
        return 'Eq'
    elif op_type is ast.NotEq:
        return 'NotEq'
    elif op_type is ast.Lt:
        return 'Lt'
    elif op_type is ast.LtE:
        return 'LtE'
    elif op_type is ast.Gt:
        return 'Gt'
    elif op_type is ast.GtE:
        return 'GtE'
    else:
        raise ValueError(f"unsupported operator {op_type}")
