import os

import pytest

from verta import ModelDBClient


HOST_ENV_VAR = "MODELDB_HOST"
PORT_ENV_VAR = "MODELDB_PORT"
EMAIL_ENV_VAR = "MODELDB_EMAIL"
DEV_KEY_ENV_VAR = "MODELDB_DEV_KEY"

DEFAULT_HOST = "localhost"
DEFAULT_PORT = "8080"
DEFAULT_EMAIL = None
DEFAULT_DEV_KEY = None


@pytest.fixture(scope='session')
def host():
    return os.environ.get(HOST_ENV_VAR, DEFAULT_HOST)


@pytest.fixture(scope='session')
def port():
    return os.environ.get(PORT_ENV_VAR, DEFAULT_PORT)


@pytest.fixture(scope='session')
def email():
    return os.environ.get(EMAIL_ENV_VAR, DEFAULT_EMAIL)


@pytest.fixture(scope='session')
def dev_key():
    return os.environ.get(DEV_KEY_ENV_VAR, DEFAULT_DEV_KEY)


@pytest.fixture
def client(host, port, email, dev_key):
    client = ModelDBClient(host, port, email, dev_key)

    return client


@pytest.fixture
def run(client):
    client.set_project()
    client.set_experiment()
    return client.set_experiment_run()
