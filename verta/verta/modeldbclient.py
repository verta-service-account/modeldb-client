from typing import Any, Union, Tuple

import time

import grpc

from ._grpc import CommonService_pb2, CommonService_pb2_grpc
from ._grpc import ExperimentService_pb2, ExperimentService_pb2_grpc
from ._grpc import ProjectService_pb2, ProjectService_pb2_grpc
from ._grpc import ExperimentRunService_pb2, ExperimentRunService_pb2_grpc


class ModelDBClient:
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = "8085"

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

    def _connect(func):
        def wrapper(self, *args, **kwargs):
            channel = grpc.insecure_channel(self.socket); print("channel established")
            result = func(self, *args, **kwargs, channel=channel)
            channel.close(); print("channel closed")
            return result
        return wrapper

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

    @_connect
    def set_experiment_runs(self, *, channel):
        if self.proj is None:
            raise AttributeError("a project must first in progress")

        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=self.proj.id)
        response = stub.getExperimentRunsInProject(msg)  # TODO: verify response

        self.expt_runs = [ExperimentRun(self.socket,
                                        self.proj.id, self.expt.id,
                                        expt_run.name)
                          for expt_run in response.experiment_runs]


class Project:
    def __init__(self, socket, proj_name=None, *, proj_id=None):
        _assert_maximum_one(proj_name=proj_name, proj_id=proj_id)

        self.socket = socket

        if proj_id is None:  # use `proj_name`
            if proj_name is None:
                proj_name = Project.generate_default_name()

            proj = self.get(proj_name)
            if proj is None:
                proj = self.create(proj_name=proj_name)
        else:  # use `proj_id`
            proj = self.get(proj_id=proj_id)
            if proj is None:
                raise ValueError(f"Project with id {proj_id} does not exist")

        self.id = proj.id

    @staticmethod
    def generate_default_name():
        return "Project {}".format(int(time.time()))

    def _connect(func):
        def wrapper(self, *args, **kwargs):
            channel = grpc.insecure_channel(self.socket); print("channel established")
            result = func(self, *args, **kwargs, channel=channel)
            channel.close(); print("channel closed")

            # attempt to use closed channel
            stub = ProjectService_pb2_grpc.ProjectServiceStub(channel)
            msg = ProjectService_pb2.CreateProject(name="PLEASE WORK")
            try:
                response = stub.createProject(msg)  # TODO: verify response
            except ValueError as e:
                print(e)

            return result
        return wrapper

    @_connect
    def get(self, proj_name=None, *, channel, proj_id=None):
        _assert_exactly_one(proj_name=proj_name, proj_id=proj_id)

        stub = ProjectService_pb2_grpc.ProjectServiceStub(channel)
        if proj_id is None:  # use `proj_name`
            msg = ProjectService_pb2.GetProject(name=proj_name)
            try:
                response = stub.getProjectByName(msg)  # TODO: verify response
            except grpc.RpcError:
                return None
            else:
                return response.project
        else:  # use `proj_id`
            msg = ProjectService_pb2.GetProject(id=proj_id)
            try:
                response = stub.getProjectById(msg)  # TODO: verify response
            except grpc.RpcError:
                return None
            else:
                return response.project

    @_connect
    def create(self, proj_name, *, channel):
        stub = ProjectService_pb2_grpc.ProjectServiceStub(channel)
        msg = ProjectService_pb2.CreateProject(name=proj_name)
        response = stub.createProject(msg)  # TODO: verify response
        return response.project


class Experiment:
    def __init__(self, socket, proj_id, expt_name=None, *, expt_id=None):
        _assert_maximum_one(expt_name=expt_name, expt_id=expt_id)

        self.socket = socket

        if expt_id is None:  # use `expt_name`
            if expt_name is None:
                expt_name = Experiment.generate_default_name()

            expt = self.get(proj_id, expt_name)
            if expt is None:
                expt = self.create(proj_id, expt_name)
        else:  # use `expt_id`
            expt = self.get(proj_id, expt_id=expt_id)
            if expt is None:
                raise ValueError(f"Experiment with id {expt_id} does not exist")

        self.id = expt.id

    @staticmethod
    def generate_default_name():
        return "Experiment {}".format(int(time.time()))

    def _connect(func):
        def wrapper(self, *args, **kwargs):
            channel = grpc.insecure_channel(self.socket); print("channel established")
            result = func(self, *args, **kwargs, channel=channel)
            channel.close(); print("channel closed")

            # attempt to use closed channel
            stub = ProjectService_pb2_grpc.ProjectServiceStub(channel)
            msg = ProjectService_pb2.CreateProject(name="PLEASE WORK")
            try:
                response = stub.createProject(msg)  # TODO: verify response
            except ValueError as e:
                print(e)

            return result
        return wrapper

    @_connect
    def get(self, proj_id, expt_name=None, *, channel, expt_id=None):
        _assert_exactly_one(expt_name=expt_name, expt_id=expt_id)

        stub = ExperimentService_pb2_grpc.ExperimentServiceStub(channel)
        if expt_id is None:  # use `expt_name`
            msg = ExperimentService_pb2.GetExperimentByName(project_id=proj_id, name=expt_name)
            try:
                response = stub.getExperimentByName(msg)  # TODO: verify response
            except grpc.RpcError:
                return None
            else:
                return response.experiment
        else:  # use `expt_id`
            msg = ExperimentService_pb2.GetExperiment(id=expt_id)
            try:
                response = stub.getExperiment(msg)  # TODO: verify response
            except grpc.RpcError:
                return None
            else:
                return response.experiment

    @_connect
    def create(self, proj_id, expt_name, *, channel):
        stub = ExperimentService_pb2_grpc.ExperimentServiceStub(channel)
        msg = ExperimentService_pb2.CreateExperiment(project_id=proj_id,
                                                     name=expt_name)
        response = stub.createExperiment(msg)  # TODO: verify response
        return response.experiment


class ExperimentRun:
    def __init__(self, socket, proj_id, expt_id, expt_run_name=None, *, expt_run_id=None):
        _assert_maximum_one(expt_run_name=expt_run_name, expt_run_id=expt_run_id)

        self.socket = socket

        if expt_run_id is None:  # use `expt_run_name`
            if expt_run_name is None:
                expt_run_name = ExperimentRun.generate_default_name()

            expt_run = self.get(proj_id, expt_run_name)
            if expt_run is None:
                expt_run = self.create(proj_id, expt_id, expt_run_name)
        else:  # use `expt_run_id`
            expt_run = self.get(proj_id, expt_run_id=expt_run_id)
            if expt_run is None:
                raise ValueError(f"ExperimentRun with id {expt_run_id} does not exist")

        self.id = expt_run.id

    @staticmethod
    def generate_default_name():
        return "ExperimentRun {}".format(int(time.time()))

    def _connect(func):
        def wrapper(self, *args, **kwargs):
            channel = grpc.insecure_channel(self.socket); print("channel established")
            result = func(self, *args, **kwargs, channel=channel)
            channel.close(); print("channel closed")

            # attempt to use closed channel
            stub = ProjectService_pb2_grpc.ProjectServiceStub(channel)
            msg = ProjectService_pb2.CreateProject(name="PLEASE WORK")
            try:
                response = stub.createProject(msg)  # TODO: verify response
            except ValueError as e:
                print(e)

            return result
        return wrapper

    @_connect
    def get(self, proj_id, expt_run_name=None, *, channel, expt_run_id=None):
        _assert_exactly_one(expt_run_name=expt_run_name, expt_run_id=expt_run_id)

        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        if expt_run_id is None:  # use `expt_run_name`
            msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=proj_id)
            response = stub.getExperimentRunsInProject(msg)  # TODO: verify response
            result = [expt_run for expt_run in response.experiment_runs if expt_run.name == expt_run_name]
            return result[-1] if result else None
        else:  # use `expt_run_id`
            msg = ExperimentRunService_pb2.GetExperimentRun(id=expt_run_id)
            response = stub.getExperimentRun(msg)  # TODO: verify response
            try:
                return response.experiment_run
            except AttributeError:
                return None

    @_connect
    def create(self, proj_id, expt_id, expt_run_name, *, channel):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.CreateExperimentRun(project_id=proj_id,
                                                           experiment_id=expt_id,
                                                           name=expt_run_name)
        response = stub.createExperimentRun(msg)  # TODO: verify response
        return response.experiment_run

    @_connect
    def log_attribute(self, name, value, *, channel):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.LogAttribute(id=self.id,
                                                         attribute=attribute)
        response = stub.logAttribute(msg)  # TODO: verify response

    @_connect
    def get_attributes(self, *, channel):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetAttributes(id=self.id)
        response = stub.getAttributes(msg)  # TODO: verify response

        return {attribute.key: _cast_to_python(attribute.value, attribute.value_type)
                for attribute in response.attributes}

    @_connect
    def log_metric(self, name, value, *, channel):
        proto_type = _get_proto_type(value)
        metric = CommonService_pb2.KeyValue(key=name, value=str(value),
                                            value_type=proto_type)
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.LogMetric(id=self.id,
                                                 metric=metric)
        response = stub.logMetric(msg)  # TODO: verify response

    @_connect
    def get_metrics(self, *, channel):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetMetrics(id=self.id)
        response = stub.getMetrics(msg)  # TODO: verify response

        return {metric.key: _cast_to_python(metric.value, metric.value_type)
                for metric in response.metrics}

    @_connect
    def log_hyperparameter(self, name, value, *, channel):
        proto_type = _get_proto_type(value)
        hyperparameter = CommonService_pb2.KeyValue(key=name, value=str(value),
                                                    value_type=proto_type)
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.LogHyperparameter(id=self.id,
                                                         hyperparameter=hyperparameter)
        response = stub.logHyperparameter(msg)  # TODO: verify response

    @_connect
    def get_hyperparameters(self, *, channel):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetHyperparameters(id=self.id)
        response = stub.getHyperparameters(msg)  # TODO: verify response

        return {hyperparameter.key: _cast_to_python(hyperparameter.value, hyperparameter.value_type)
                for hyperparameter in response.hyperparameters}

    @_connect
    def log_dataset(self, name, path, *, channel):
        dataset = CommonService_pb2.Artifact(key=name, path=path,
                                             artifact_type=CommonService_pb2.ArtifactTypeEnum.DATA)
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.LogDataset(id=self.id,
                                                  dataset=dataset)
        response = stub.logDataset(msg)  # TODO: verify response

    @_connect
    def get_datasets(self, *, channel):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetDatasets(id=self.id)
        response = stub.getDatasets(msg)  # TODO: verify response

        return {dataset.key: dataset.path for dataset in response.datasets}

    @_connect
    def log_model(self, name, path, *, channel):
        model = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.MODEL)
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=model)
        response = stub.logArtifact(msg)

    @_connect
    def get_models(self, *, channel):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        response = stub.getArtifacts(msg)

        return {artifact.key: artifact.path
                for artifact in response.artifacts
                if artifact.artifact_type == CommonService_pb2.ArtifactTypeEnum.MODEL}

    @_connect
    def log_image(self, name, path, * channel):
        image = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.IMAGE)
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=image)
        response = stub.logArtifact(msg)

    @_connect
    def get_image(self, name, *, channel):  # TODO: this, but better
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        response = stub.getArtifacts(msg)

        return [artifact.path
                for artifact in response.artifacts
                if artifact.artifact_type == CommonService_pb2.ArtifactTypeEnum.IMAGE
                and artifact.key == name][0]

    @_connect
    def log_observation(self, name, value, *, channel):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        observation = ExperimentRunService_pb2.Observation(attribute=attribute)  # TODO: support Artifacts
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.LogObservation(id=self.id,
                                                      observation=observation)
        response = stub.logObservation(msg)  # TODO: verify response

    @_connect
    def get_observations(self, name, *, channel):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetObservations(id=self.id,
                                                       observation_key=name)
        response = stub.getObservations(msg)  # TODO: verify response

        return [_cast_to_python(observation.attribute.value, observation.attribute.value_type)
                for observation in response.observations]  # TODO: support Artifacts


def _get_proto_type(val):
    if isinstance(val, float) or isinstance(val, int):
        return CommonService_pb2.ValueTypeEnum.NUMBER
    else:
        return CommonService_pb2.ValueTypeEnum.STRING


def _cast_to_python(val, proto_type):
    if proto_type is CommonService_pb2.ValueTypeEnum.NUMBER:
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
