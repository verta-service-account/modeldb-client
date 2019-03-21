import os

import pytest

from verta import ModelDBClient


ENV_VARS = {
    'host': "MODELDB_HOST",
    'port': "MODELDB_PORT",
    'email': "MODELDB_EMAIL",
    'dev_key': "MODELDB_DEV_KEY",
}
DEFAULTS = {
    'host': "localhost",
    'port': "8080",
    'email': None,
    'dev_key': None,
}


@pytest.fixture
def client():
    kwargs = DEFAULTS.copy()
    for key in kwargs.keys():
        try:
            kwargs[key] = os.environ[ENV_VARS[key]]
        except KeyError:
            print("${} not found; using default value {}".format(ENV_VARS[key], kwargs[key]))
        else:
            print("${} found; using value {}".format(ENV_VARS[key], kwargs[key]))

    client = ModelDBClient(**kwargs)

    return client


@pytest.fixture
def run(client):
    client.set_project()
    client.set_experiment()
    return client.set_experiment_run()
