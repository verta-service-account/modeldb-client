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
        self.channel = grpc.insecure_channel(host + ":" + port)

        self.proj = None
        self.expt = None
        self.expt_runs = []

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_val, traceback):
        self.disconnect()

    def disconnect(self):
        """Close channel and clear instance attributes."""
        self.channel.close()
        self.channel = None

        self.proj = None
        self.expt = None
        self.expt_runs = []

    def set_project(self, proj_name=None):
        # TODO: handle case when project is already in progress

        proj = Project(self.channel, proj_name)

        self.proj = proj
        return proj

    def set_experiment(self, expt_name=None):
        # TODO: handle case when experiment is already in progress
        if self.proj is None:
            raise AttributeError("a project must first in progress")

        expt = Experiment(self.channel, self.proj.id, expt_name)

        self.expt = expt
        return expt

    def set_experiment_run(self, expt_run_name=None):
        if self.proj is None:
            raise AttributeError("a project must first in progress")
        if self.expt is None:
            raise AttributeError("an experiment must first in progress")

        expt_run = ExperimentRun(self.channel,
                                 self.proj.id, self.expt.id,
                                 expt_run_name)

        self.expt_runs.append(expt_run)
        return expt_run

    def set_experiment_runs(self):
        if self.proj is None:
            raise AttributeError("a project must first in progress")

        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(self.channel)
        msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=self.proj.id)
        response = stub.getExperimentRunsInProject(msg)  # TODO: verify response

        self.expt_runs = [ExperimentRun(self.channel,
                                        self.proj.id, self.expt.id,
                                        expt_run.name)
                          for expt_run in response.experiment_runs]


class Project:
    def __init__(self, channel, proj_name=None):
        if proj_name is None:
            proj_name = Project.generate_default_name()

        stub = ProjectService_pb2_grpc.ProjectServiceStub(channel)

        try:
            proj = Project.get(channel, proj_name)
        except grpc.RpcError:  # when no Projects in Mongo
            proj = None
        if proj is None:
            msg = ProjectService_pb2.CreateProject(name=proj_name)
            response = stub.createProject(msg)  # TODO: verify response
            proj = response.project

        self.stub = stub
        self.id = proj.id

    @staticmethod
    def get(channel, proj_name):
        stub = ProjectService_pb2_grpc.ProjectServiceStub(channel)
        msg = ProjectService_pb2.GetProjects()
        response = stub.getProjects(msg)  # TODO: verify response

        result = [proj for proj in response.projects if proj.name == proj_name]
        return result[-1] if len(result) else None

    @staticmethod
    def generate_default_name():
        return "Project {}".format(int(time.time()))


class Experiment:
    def __init__(self, channel, proj_id, expt_name=None):
        if expt_name is None:
            expt_name = Experiment.generate_default_name()

        stub = ExperimentService_pb2_grpc.ExperimentServiceStub(channel)

        try:
            expt = Experiment.get(channel, proj_id, expt_name)
        except grpc.RpcError:  # when Project has no Experiments
            expt = None
        if expt is None:
            msg = ExperimentService_pb2.CreateExperiment(project_id=proj_id,
                                                         name=expt_name)
            response = stub.createExperiment(msg)  # TODO: verify response
            expt = response.experiment

        self.stub = stub
        self.id = expt.id

    @staticmethod
    def get(channel, proj_id, expt_name):
        stub = ExperimentService_pb2_grpc.ExperimentServiceStub(channel)
        msg = ExperimentService_pb2.GetExperimentsInProject(project_id=proj_id)
        response = stub.getExperimentsInProject(msg)  # TODO: verify response

        result = [expt for expt in response.experiments if expt.name == expt_name]
        return result[-1] if len(result) else None

    @staticmethod
    def generate_default_name():
        return "Experiment {}".format(int(time.time()))


class ExperimentRun:
    def __init__(self, channel, proj_id, expt_id, expt_run_name=None):
        if expt_run_name is None:
            expt_run_name = ExperimentRun.generate_default_name()

        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)

        try:
            expt_run = ExperimentRun.get(channel, proj_id, expt_run_name)
        except grpc.RpcError:  # when Project has no ExperimentRuns
            expt_run = None
        if expt_run is None:
            msg = ExperimentRunService_pb2.CreateExperimentRun(project_id=proj_id,
                                                               experiment_id=expt_id,
                                                               name=expt_run_name)
            response = stub.createExperimentRun(msg)  # TODO: verify response
            expt_run = response.experiment_run

        self.stub = stub
        self.id = expt_run.id

    @staticmethod
    def get(channel, proj_id, expt_run_name):
        stub = ExperimentRunService_pb2_grpc.ExperimentRunServiceStub(channel)
        msg = ExperimentRunService_pb2.GetExperimentRunsInProject(project_id=proj_id)
        response = stub.getExperimentRunsInProject(msg)  # TODO: verify response

        result = [expt_run for expt_run in response.experiment_runs if expt_run.name == expt_run_name]
        return result[-1] if len(result) else None

    @staticmethod
    def generate_default_name():
        return "ExperimentRun {}".format(int(time.time()))

    def log_attribute(self, name, value):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        msg = ExperimentRunService_pb2.LogAttribute(id=self.id,
                                                         attribute=attribute)
        response = self.stub.logAttribute(msg)  # TODO: verify response

    def get_attributes(self):
        msg = ExperimentRunService_pb2.GetAttributes(id=self.id)
        response = self.stub.getAttributes(msg)  # TODO: verify response

        return {attribute.key: _cast_to_python(attribute.value, attribute.value_type)
                for attribute in response.attributes}

    def log_metric(self, name, value):
        proto_type = _get_proto_type(value)
        metric = CommonService_pb2.KeyValue(key=name, value=str(value),
                                            value_type=proto_type)
        msg = ExperimentRunService_pb2.LogMetric(id=self.id,
                                                 metric=metric)
        response = self.stub.logMetric(msg)  # TODO: verify response

    def get_metrics(self):
        msg = ExperimentRunService_pb2.GetMetrics(id=self.id)
        response = self.stub.getMetrics(msg)  # TODO: verify response

        return {metric.key: _cast_to_python(metric.value, metric.value_type)
                for metric in response.metrics}

    def log_hyperparameter(self, name, value):
        proto_type = _get_proto_type(value)
        hyperparameter = CommonService_pb2.KeyValue(key=name, value=str(value),
                                                    value_type=proto_type)
        msg = ExperimentRunService_pb2.LogHyperparameter(id=self.id,
                                                         hyperparameter=hyperparameter)
        response = self.stub.logHyperparameter(msg)  # TODO: verify response

    def get_hyperparameters(self):
        msg = ExperimentRunService_pb2.GetHyperparameters(id=self.id)
        response = self.stub.getHyperparameters(msg)  # TODO: verify response

        return {hyperparameter.key: _cast_to_python(hyperparameter.value, hyperparameter.value_type)
                for hyperparameter in response.hyperparameters}

    def log_dataset(self, name, path):
        dataset = CommonService_pb2.Artifact(key=name, path=path,
                                             artifact_type=CommonService_pb2.ArtifactTypeEnum.DATA)
        msg = ExperimentRunService_pb2.LogDataset(id=self.id,
                                                  dataset=dataset)
        response = self.stub.logDataset(msg)  # TODO: verify response

    def get_datasets(self):
        msg = ExperimentRunService_pb2.GetDatasets(id=self.id)
        response = self.stub.getDatasets(msg)  # TODO: verify response

        return {dataset.key: dataset.path for dataset in response.datasets}

    def log_model(self, name, path):
        model = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.MODEL)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=model)
        response = self.stub.logArtifact(msg)

    def get_models(self):
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        response = self.stub.getArtifacts(msg)

        return {artifact.key: artifact.path
                for artifact in response.artifacts
                if artifact.artifact_type == CommonService_pb2.ArtifactTypeEnum.MODEL}

    def log_image(self, name, path):
        image = CommonService_pb2.Artifact(key=name, path=path,
                                           artifact_type=CommonService_pb2.ArtifactTypeEnum.IMAGE)
        msg = ExperimentRunService_pb2.LogArtifact(id=self.id,
                                                   artifact=image)
        response = self.stub.logArtifact(msg)

    def get_image(self, name):  # TODO: this, but better
        msg = ExperimentRunService_pb2.GetArtifacts(id=self.id)
        response = self.stub.getArtifacts(msg)

        return [artifact.path
                for artifact in response.artifacts
                if artifact.artifact_type == CommonService_pb2.ArtifactTypeEnum.IMAGE
                and artifact.key == name][0]

    def log_observation(self, name, value):
        proto_type = _get_proto_type(value)
        attribute = CommonService_pb2.KeyValue(key=name, value=str(value),
                                               value_type=proto_type)
        observation = ExperimentRunService_pb2.Observation(attribute=attribute)  # TODO: support Artifacts
        msg = ExperimentRunService_pb2.LogObservation(id=self.id,
                                                      observation=observation)
        response = self.stub.logObservation(msg)  # TODO: verify response

    def get_observations(self, name):
        msg = ExperimentRunService_pb2.GetObservations(id=self.id,
                                                       observation_key=name)
        response = self.stub.getObservations(msg)  # TODO: verify response

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
