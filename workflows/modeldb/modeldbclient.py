import os, sys

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

        self.proj_stub = ProjectService_pb2_grpc.ProjectServiceStub(self.channel)
        self.expt_stub = ExperimentService_pb2_grpc.ExperimentServiceStub(self.channel)

        self.proj_id = None
        self.expt_id = None
        self.expt_runs = []

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_val, traceback):
        self.disconnect()

    def disconnect(self):
        """Close channel and clear instance attributes."""
        self.channel.close()
        self.channel = None

        self.proj_stub = None
        self.expt_stub = None

        self.proj_id = None
        self.expt_id = None
        self.expt_runs = []

    def start_project(self, proj_name):
        # TODO: handle case when project is already in progress
        msg = ProjectService_pb2.CreateProject(name=proj_name)
        response = self.proj_stub.createProject(msg)  # TODO: verify response

        self.proj_id = response.project.id
        return self.proj_id

    def start_experiment(self, expt_name):
        # TODO: handle case when experiment is already in progress
        if self.proj_id is None:
            raise AttributeError("a project must first in progress")
        msg = ExperimentService_pb2.CreateExperiment(project_id=self.proj_id,
                                                     name=expt_name)
        response = self.expt_stub.createExperiment(msg)  # TODO: verify response

        self.expt_id = response.experiment.id
        return self.expt_id

    def create_experiment_run(self, expt_run_name):
        if self.proj_id is None:
            raise AttributeError("a project must first in progress")
        if self.expt_id is None:
            raise AttributeError("an experiment must first in progress")
        expt_run = ExperimentRun(self.channel,
                                 self.proj_id, self.expt_id,
                                 expt_run_name)

        self.expt_runs.append(expt_run)
        return expt_run
