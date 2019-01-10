import os, sys

import grpc

sys.path.append(os.path.join("..", "proto"))
import CommonService_pb2, CommonService_pb2_grpc
import ExperimentService_pb2, ExperimentService_pb2_grpc
import ProjectService_pb2, ProjectService_pb2_grpc
import ExperimentRunService_pb2, ExperimentRunService_pb2_grpc
