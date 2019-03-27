import random

import requests


def gen_str(length=8):
    return ''.join([chr(random.randrange(97, 123))
                    for _
                    in range(length)])


def gen_int(start=10, stop=None):
    return random.randrange(start, stop)


def gen_float(start=1, stop=None):
    if stop is None:
        return random.random()*start
    else:
        return random.uniform(start, stop)


def delete_project(id_, client):
    response = requests.get("http://{}/v1/experiment/getExperimentsInProject".format(client._socket),
                            params={'project_id': id_}, headers=client._auth)
    response.raise_for_status()
    for experiment in response.json().get('experiments', []):
        delete_experiment(experiment['id'], client)

    response = requests.delete("http://{}/v1/project/deleteProject".format(client._socket),
                               json={'id': id_}, headers=client._auth)
    response.raise_for_status()


def delete_experiment(id_, client):
    response = requests.get("http://{}/v1/experiment-run/getExperimentRunsInExperiment".format(client._socket),
                            params={'experiment_id': id_}, headers=client._auth)
    response.raise_for_status()
    for experiment_run in response.json().get('experiment_runs', []):
        delete_experiment_run(experiment_run['id'], client)

    response = requests.delete("http://{}/v1/experiment/deleteExperiment".format(client._socket),
                               json={'id': id_}, headers=client._auth)
    response.raise_for_status()


def delete_experiment_run(id_, client):
    response = requests.delete("http://{}/v1/experiment-run/deleteExperimentRun".format(client._socket),
                               json={'id': id_}, headers=client._auth)
    response.raise_for_status()
