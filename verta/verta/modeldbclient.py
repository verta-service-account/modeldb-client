from typing import Any, Union, Tuple

import time

import requests
import json

from google.protobuf import json_format

from ._grpc import CommonService_pb2
from ._grpc import ExperimentService_pb2
from ._grpc import ProjectService_pb2
from ._grpc import ExperimentRunService_pb2


class ModelDBClient:
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = "8080"

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        self.socket = f"{host}:{port}"

        self.proj = None
        self.expt = None
        self.expt_runs = []

    def __getstate__(self):
        return {'socket': self.socket,
                'proj_id': self.proj.id if self.proj is not None else None,
                'expt_id': self.expt.id if self.expt is not None else None,
                'expt_run_ids': [expt_run.id for expt_run in self.expt_runs]}

    def __setstate__(self, state):
        self.socket = state['socket']

        self.proj = Project(self.socket, proj_id=state['proj_id']) if state['proj_id'] is not None else None
        self.expt = Experiment(self.socket, state['proj_id'], expt_id=state['expt_id']) if state['expt_id'] is not None else None
        self.expt_runs = [ExperimentRun(self.socket, state['proj_id'], state['expt_id'], expt_run_id=expt_run_id)
                          for expt_run_id in state['expt_run_ids']]

    def set_project(self, proj_name=None):
        # TODO: handle case when project is already in progress

        proj = Project(self.socket, proj_name)

        self.proj = proj
        return proj

    def set_experiment(self, expt_name=None):
        # TODO: handle case when experiment is already in progress
        if self.proj is None:
            raise AttributeError("a project must first in progress")

        expt = Experiment(self.socket, self.proj.id, expt_name)

        self.expt = expt
        return expt

    def set_experiment_run(self, expt_run_name=None):
        if self.proj is None:
            raise AttributeError("a project must first in progress")
        if self.expt is None:
            raise AttributeError("an experiment must first in progress")

        expt_run = ExperimentRun(self.socket,
                                 self.proj.id, self.expt.id,
                                 expt_run_name)

        self.expt_runs.append(expt_run)
        return expt_run

    def set_experiment_runs(self):
        if self.proj is None:
            raise AttributeError("a project must first in progress")

        msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=self.proj.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getExperimentRunsInProject",
                                 json=data).json()  # TODO: verify response

        self.expt_runs = [ExperimentRun(self.socket,
                                        self.proj.id, self.expt.id,
                                        expt_run['name'])
                          for expt_run in response['experiment_runs']]


class Project:
    def __init__(self, socket, proj_name=None, *, proj_id=None):
        _assert_maximum_one(proj_name=proj_name, proj_id=proj_id)

        if proj_id is None:  # use `proj_name`
            if proj_name is None:
                proj_name = Project.generate_default_name()

            proj = Project.get(socket, proj_name)
            if proj is None:
                msg = ProjectService_pb2.CreateProject(name=proj_name)
                data = json.loads(json_format.MessageToJson(msg))
                response = requests.post(f"http://{socket}/v1/example/createProject",
                                         json=data).json()  # TODO: verify response
                proj = response['project']
        else:  # use `proj_id`
            proj = Project.get(socket, proj_id=proj_id)
            if proj is None:
                raise ValueError(f"Project with id {proj_id} does not exist")

        self.socket = socket
        self.id = proj['id']

    @staticmethod
    def get(socket, proj_name=None, *, proj_id=None):
        _assert_exactly_one(proj_name=proj_name, proj_id=proj_id)

        if proj_id is None:  # use `proj_name`
            msg = ProjectService_pb2.GetProject(name=proj_name)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.post(f"http://{socket}/v1/example/getProjectByName",
                                     json=data).json()  # TODO: verify response
            if 'error' in response and response['error'] == 'Project not found in database':
                return None
            else:
                return response['project']
        else:  # use `proj_id`
            msg = ProjectService_pb2.GetProject(id=proj_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.post(f"http://{socket}/v1/example/getProjectById",
                                     json=data).json()  # TODO: verify response
            if 'error' in response and response['error'] == 'Project not found in database':
                return None
            else:
                return response['project']

    @staticmethod
    def generate_default_name():
        return "Project {}".format(int(time.time()))


class Experiment:
    def __init__(self, socket, proj_id, expt_name=None, *, expt_id=None):
        _assert_maximum_one(expt_name=expt_name, expt_id=expt_id)

        if expt_id is None:  # use `expt_name`
            if expt_name is None:
                expt_name = Experiment.generate_default_name()

            expt = Experiment.get(socket, proj_id, expt_name)
            if expt is None:
                msg = ExperimentService_pb2.CreateExperiment(project_id=proj_id,
                                                             name=expt_name)
                data = json.loads(json_format.MessageToJson(msg))
                response = requests.post(f"http://{socket}/v1/example/createExperiment",
                                         json=data).json()  # TODO: verify response
                expt = response['experiment']
        else:  # use `expt_id`
            expt = Experiment.get(socket, proj_id, expt_id=expt_id)
            if expt is None:
                raise ValueError(f"Experiment with id {expt_id} does not exist")

        self.socket = socket
        self.id = expt['id']

    @staticmethod
    def get(socket, proj_id, expt_name=None, *, expt_id=None):
        _assert_exactly_one(expt_name=expt_name, expt_id=expt_id)

        if expt_id is None:  # use `expt_name`
            msg = ExperimentService_pb2.GetExperimentByName(project_id=proj_id, name=expt_name)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.post(f"http://{socket}/v1/example/getExperimentByName",
                                     json=data).json()  # TODO: verify response
            if 'error' in response and response['error'].startswith("Experiment not found"):
                return None
            else:
                return response['experiment']
        else:  # use `expt_id`
            msg = ExperimentService_pb2.GetExperiment(id=expt_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.post(f"http://{socket}/v1/example/getExperiment",
                                     json=data).json()  # TODO: verify response
            if 'error' in response and response['error'].startswith("Experiment not found"):
                return None
            else:
                return response['experiment']

    @staticmethod
    def generate_default_name():
        return "Experiment {}".format(int(time.time()))


class ExperimentRun:
    def __init__(self, socket, proj_id, expt_id, expt_run_name=None, *, expt_run_id=None):
        _assert_maximum_one(expt_run_name=expt_run_name, expt_run_id=expt_run_id)

        if expt_run_id is None:  # use `expt_run_name`
            if expt_run_name is None:
                expt_run_name = ExperimentRun.generate_default_name()

            expt_run = ExperimentRun.get(socket, proj_id, expt_run_name)
            if expt_run is None:
                msg = ExperimentRunService_pb2.CreateExperimentRun(project_id=proj_id,
                                                                   experiment_id=expt_id,
                                                                   name=expt_run_name)
                data = json.loads(json_format.MessageToJson(msg))
                response = requests.post(f"http://{socket}/v1/example/createExperimentRun",
                                       json=data).json()  # TODO: verify response
                expt_run = response['experiment_run']
        else:  # use `expt_run_id`
            expt_run = ExperimentRun.get(socket, proj_id, expt_run_id=expt_run_id)
            if expt_run is None:
                raise ValueError(f"ExperimentRun with id {expt_run_id} does not exist")

        self.socket = socket
        self.id = expt_run['id']

    @staticmethod
    def get(socket, proj_id, expt_run_name=None, *, expt_run_id=None):
        _assert_exactly_one(expt_run_name=expt_run_name, expt_run_id=expt_run_id)

        if expt_run_id is None:  # use `expt_run_name`
            msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=proj_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.post(f"http://{socket}/v1/example/getExperimentRunsInProject",
                                     json=data).json()  # TODO: verify response
            if 'experiment_runs' in response:
                result = [expt_run for expt_run in response['experiment_runs'] if expt_run['name'] == expt_run_name]
                return result[-1] if len(result) else None
            else:  # no expt_runs in proj
                return None
        else:  # use `expt_run_id`
            msg = ExperimentRunService_pb2.GetExperimentRun(id=expt_run_id)
            data = json.loads(json_format.MessageToJson(msg))
            response = requests.post(f"http://{socket}/v1/example/getExperimentRun",
                                     json=data).json()  # TODO: verify response
            if 'error' in response and response['error'].startswith("ExperimentRun not found"):
                return None
            else:
                return response['experiment']

    @staticmethod
    def generate_default_name():
        return "ExperimentRun {}".format(int(time.time()))

    def log_attribute(self, name, value):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        msg = ExperimentRunService_pb2.LogAttribute(id=self.id,
                                                    attribute=attribute)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/logAttribute",
                                 json=data).json()  # TODO: verify response

    def get_attributes(self):
        msg = ExperimentRunService_pb2.GetAttributes(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getAttributes",
                                 json=data).json()  # TODO: verify response

        return {attribute['key']: _cast_to_python(attribute['value'], attribute['value_type'])
                for attribute in response['attributes']}

    def log_metric(self, name, value):
        proto_type = _get_proto_type(value)
        metric = CommonService_pb2.KeyValue(key=name, value=str(value),
                                            value_type=proto_type)
        msg = ExperimentRunService_pb2.LogMetric(id=self.id,
                                                 metric=metric)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/logMetric",
                                 json=data).json()  # TODO: verify response

    def get_metrics(self):
        msg = ExperimentRunService_pb2.GetMetrics(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getMetrics",
                                 json=data).json()  # TODO: verify response

        return {metric['key']: _cast_to_python(metric['value'], metric['value_type'])
                for metric in response['metrics']}

    def log_hyperparameter(self, name, value):
        proto_type = _get_proto_type(value)
        hyperparameter = CommonService_pb2.KeyValue(key=name, value=str(value),
                                                    value_type=proto_type)
        msg = ExperimentRunService_pb2.LogHyperparameter(id=self.id,
                                                         hyperparameter=hyperparameter)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/logHyperparameter",
                                 json=data).json()  # TODO: verify response

    def get_hyperparameters(self):
        msg = ExperimentRunService_pb2.GetHyperparameters(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getHyperparameters",
                                 json=data).json()  # TODO: verify response

        return {hyperparameter['key']: _cast_to_python(hyperparameter['value'], hyperparameter['value_type'])
                for hyperparameter in response['hyperparameters']}

    def log_dataset(self, name, path):
        dataset = CommonService_pb2.Artifact(key=name, path=path,
                                             artifact_type=CommonService_pb2.ArtifactTypeEnum.DATA)
        msg = ExperimentRunService_pb2.LogDataset(id=self.id,
                                                  dataset=dataset)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/logDataset",
                                 json=data).json()  # TODO: verify response

    def get_datasets(self):
        msg = ExperimentRunService_pb2.GetDatasets(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getDatasets",
                                 json=data).json()  # TODO: verify response

        return {dataset['key']: dataset['path'] for dataset in response['datasets']}

    def log_model(self, name, path):
        model = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.MODEL)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=model)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/logModel",
                                 json=data).json()  # TODO: verify response

    def get_models(self):
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getModels",
                                 json=data).json()  # TODO: verify response

        return {artifact['key']: artifact['path']
                for artifact in response['artifacts']
                if artifact['artifact_type'] == 'MODEL'}

    def log_image(self, name, path):
        image = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.IMAGE)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=image)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/logImage",
                                 json=data).json()  # TODO: verify response

    def get_image(self, name):  # TODO: this, but better
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getImage",
                                 json=data).json()  # TODO: verify response

        return [artifact['path']
                for artifact in response['artifacts']
                if artifact['artifact_type'] == 'IMAGE'
                and artifact['key'] == name][0]

    def log_observation(self, name, value):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        observation = ExperimentRunService_pb2.Observation(attribute=attribute)  # TODO: support Artifacts
        msg = ExperimentRunService_pb2.LogObservation(id=self.id,
                                                      observation=observation)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/logObservation",
                                 json=data).json()  # TODO: verify response

    def get_observations(self, name):
        msg = ExperimentRunService_pb2.GetObservations(id=self.id,
                                                       observation_key=name)
        data = json.loads(json_format.MessageToJson(msg))
        response = requests.post(f"http://{self.socket}/v1/example/getObservations",
                                 json=data).json()  # TODO: verify response

        return [_cast_to_python(observation['attribute']['value'], observation['attribute']['value_type'])
                for observation in response['observations']]  # TODO: support Artifacts


def _get_proto_type(val):
    if isinstance(val, float) or isinstance(val, int):
        return CommonService_pb2.ValueTypeEnum.NUMBER
    else:
        return CommonService_pb2.ValueTypeEnum.STRING


def _cast_to_python(val, proto_type):
    if proto_type is 'NUMBER':
        try:
            return int(val)
        except ValueError:
            return float(val)
    else:
        return str(val)


def _assert_exactly_one(**kwargs):
    if sum([val is not None for val in kwargs.values()]) != 1:
        raise ValueError(f"only exactly one of {list(kwargs.keys())} can be not None")

def _assert_maximum_one(**kwargs):
    if sum([val is not None for val in kwargs.values()]) > 1:
        raise ValueError(f"only at most one of {list(kwargs.keys())} can be not None")
