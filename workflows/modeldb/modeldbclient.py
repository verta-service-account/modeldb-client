from typing import Any, Union, Tuple

import os, sys
import time

import grpc

sys.path.append(os.path.join("..", "grpc"))
import CommonService_pb2, CommonService_pb2_grpc
import ExperimentService_pb2, ExperimentService_pb2_grpc
import ProjectService_pb2, ProjectService_pb2_grpc
import ExperimentRunService_pb2, ExperimentRunService_pb2_grpc


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
