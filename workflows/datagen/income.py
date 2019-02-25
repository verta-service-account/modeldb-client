import os
import itertools
import time

import joblib

import numpy as np

from sklearn import model_selection
from sklearn import linear_model
from sklearn import metrics

from verta import ModelDBClient


# set these
HOST =
PORT =
EMAIL =
DEV_KEY = 
# change this, if you'd like
GRID = {'penalty': ['l1', 'l2'],
        'C': [1e-4, 1e-3, 1e-2, 1e-1]}


# no need to touch anything else
PROJECT_NAME = "Income Classification"
EXPERIMENT_NAME = "Logistic Regression"
TAGS = ["development", "deployment", "exploratory", "obsolete", "debug", "enhancement", "demo"]
LOREM = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore "
         "et dolore magna aliqua.").split()
gen_tags = lambda: np.random.choice(TAGS, size=np.random.choice(len(TAGS)), replace=False).tolist()
gen_desc = lambda: ' '.join(LOREM[:np.random.choice(len(LOREM))])

client = ModelDBClient(HOST, PORT, EMAIL, DEV_KEY)
try:
    proj = client.set_project(PROJECT_NAME, gen_desc(), gen_tags())
except ValueError:
    proj = client.set_project(PROJECT_NAME)
try:
    expt = client.set_experiment(EXPERIMENT_NAME, gen_desc(), gen_tags())
except ValueError:
    expt = client.set_experiment(EXPERIMENT_NAME)

TRAIN_DATA_PATH = os.path.join("..", "data", "census", "train.npz")
TEST_DATA_PATH = os.path.join("..", "data", "census", "test.npz")
MODEL_PATH = os.path.join("..", "output", "client-demo", "logreg_gridsearch_{}.gz")

train_data = np.load(TRAIN_DATA_PATH)
X_train, y_train = train_data['X'], train_data['y']

grid = [dict(zip(GRID.keys(), values))
        for values
        in itertools.product(*GRID.values())]

for hyperparams in grid:
    start_time = int(time.time())

    # create object to track experiment run
    run = client.set_experiment_run(None, gen_desc(), gen_tags())

    # log data
    run.log_dataset("train_data", TRAIN_DATA_PATH)
    run.log_dataset("test_data", TEST_DATA_PATH)

    # create validation split
    X_train, X_val, y_train, y_val = model_selection.train_test_split(X_train, y_train,
                                                                      test_size=0.1, shuffle=False)
    # log hyperparameters
    for key, val in hyperparams.items():
        run.log_hyperparameter(key, val)
    print(hyperparams, end=' ')

    # create and train model
    model = linear_model.LogisticRegression(solver='liblinear', **hyperparams)
    model.fit(X_train, y_train)

    # calculate and log validation accuracy
    val_acc = model.score(X_val, y_val)
    run.log_metric("val_acc", val_acc)
    print("Validation accuracy: {:.4f}".format(val_acc))

    # log model
    run.log_model("model", MODEL_PATH.format(start_time))

best_run = sorted(client.expt_runs, key=lambda run: run.get_metrics()['val_acc'])[-1]

best_hyperparams = best_run.get_hyperparameters()
best_val_acc = best_run.get_metrics()['val_acc']
print("{} Validation accuracy: {:.4f}".format(best_hyperparams, best_val_acc))

model = linear_model.LogisticRegression(solver='liblinear', **best_hyperparams)
model.fit(X_train, y_train)
train_acc = model.score(X_train, y_train)
best_run.log_metric("train_acc", train_acc)
print("Training accuracy: {:.4f}".format(train_acc))

test_data = np.load(TEST_DATA_PATH)
X_test, y_test = test_data['X'], test_data['y']
test_acc = model.score(X_test, y_test)
best_run.log_metric("test_acc", test_acc)
print("Testing accuracy: {:.4f}".format(test_acc))
