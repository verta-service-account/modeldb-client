import os
import itertools
import time

import numpy as np

import tensorflow as tf
from tensorflow import keras

import matplotlib.pyplot as plt

from verta import ModelDBClient

# set these
HOST = 
PORT = 
EMAIL =
DEV_KEY =
# change this, if you'd like
GRID = {'hidden_size': [512, 1024],
        'dropout': [.2, .4],
        'batch_size': [512]}


# no need to touch anything else
PROJECT_NAME = "MNIST Multiclassification"
EXPERIMENT_NAME = "FC-NN"
TAGS = ["development", "deployment", "exploratory", "obsolete", "debug", "enhancement", "demo"]
LOREM = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore "
         "et dolore magna aliqua.").split()
gen_tags = lambda: np.random.choice(TAGS, size=np.random.choice(len(TAGS)-1)+1, replace=False).tolist()
gen_desc = lambda: ' '.join(LOREM[:np.random.choice(len(LOREM)-1)+1])

client = ModelDBClient(HOST, PORT, EMAIL, DEV_KEY)
try:
    proj = client.set_project(PROJECT_NAME, gen_desc(), gen_tags())
except ValueError:
    proj = client.set_project(PROJECT_NAME)
try:
    expt = client.set_experiment(EXPERIMENT_NAME, gen_desc(), gen_tags())
except ValueError:
    expt = client.set_experiment(EXPERIMENT_NAME)

TRAIN_DATA_PATH = os.path.join("..", "data", "mnist", "train.npz")
TEST_DATA_PATH = os.path.join("..", "data", "mnist", "test.npz")
VAL_PLOT_PATH = os.path.join("..", "output", "val_obs_{}.png")
MODEL_PATH = os.path.join("..", "output", "tensorflow-basic_{}.hdf5")

OPTIMIZER = 'adam'
LOSS = 'sparse_categorical_crossentropy'
NUM_EPOCHS = 4
VALIDATION_SPLIT = 0.1

train_data = np.load(TRAIN_DATA_PATH)
X_train, y_train = train_data['X'], train_data['y']

grid = [dict(zip(GRID.keys(), values))
        for values
        in itertools.product(*GRID.values())]

for hyperparams in grid:
    start_time = int(time.time())

    run = client.set_experiment_run(None, gen_desc(), gen_tags())
    print(hyperparams)

    run.log_dataset("train_data", TRAIN_DATA_PATH)
    run.log_dataset("test_data", TEST_DATA_PATH)

    model = keras.models.Sequential()
    model.add(keras.layers.Flatten())
    model.add(keras.layers.Dense(hyperparams['hidden_size'], activation=tf.nn.relu))
    model.add(keras.layers.Dropout(hyperparams['dropout']))
    model.add(keras.layers.Dense(10, activation=tf.nn.softmax))
    run.log_hyperparameter("hidden_size", hyperparams['hidden_size'])
    run.log_hyperparameter("dropout", hyperparams['dropout'])

    model.compile(optimizer=OPTIMIZER,
              loss=LOSS,
              metrics=['accuracy'])
    run.log_hyperparameter("optimizer", OPTIMIZER)
    run.log_hyperparameter("loss", LOSS)

    def log_validation(epoch, logs):  # Keras will call this each epoch
        run.log_observation("val_train loss", float(logs['loss']))
        run.log_observation("val_train acc", float(logs['acc']))
        run.log_observation("val_loss", float(logs['val_loss']))
        run.log_observation("val_acc", float(logs['val_acc']))
    _ = model.fit(X_train, y_train, validation_split=VALIDATION_SPLIT,
                  batch_size=hyperparams['batch_size'], epochs=NUM_EPOCHS,
                  callbacks=[keras.callbacks.LambdaCallback(on_epoch_end=log_validation)])
    run.log_hyperparameter("batch_size", hyperparams['batch_size'])
    run.log_hyperparameter("num_epochs", NUM_EPOCHS)
    run.log_hyperparameter("validation_split", VALIDATION_SPLIT)

    plt.plot(run.get_observations("val_acc"), label="val")
    plt.plot(run.get_observations("val_train_acc"), label="train")
    plt.ylim(0, 1)
    plt.xlabel("epoch")
    plt.ylabel("accuracy")
    plt.legend(loc='best')
    run.log_image("validation_plot", VAL_PLOT_PATH.format(start_time))

    run.log_model("model", MODEL_PATH.format(start_time))
