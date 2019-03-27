.. verta documentation master file, created by
   sphinx-quickstart on Wed Mar 13 13:39:03 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Verta: Model Management & More
==============================

**Verta**'s ModelDB client strives to be lightweight and unobtrusive.

.. code-block:: python
    :emphasize-lines: 3,7-8,16,20

    # load data
    data = np.load(DATA_PATH)
    run.log_dataset("data", DATA_PATH)

    # set hyperparameters
    hyperparams = {'C': 1e-3, 'solver': "lbfgs"}
    for hyperparam, value in hyperparams:
       run.log_hyperparameter(hyperparam, value)

    # train model
    model = sklearn.linear_model.LogisticRegression(**hyperparams)
    model.fit(data['X_train'], data['y_train'])

    # test model
    test_acc = model.score(data['X_test'], data['y_test'])
    run.log_metric("test accuracy", test_acc)

    # save model
    joblib.dump(model, MODEL_PATH)
    run.log_model("model", MODEL_PATH)

Obtaining logged metadata is clean and simple.

.. code-block:: python

    >>> run.get_datasets()
    {'data': '../data/census/data.npz'}
    >>> run.get_hyperparameters()
    {'C': 1e-3, 'solver': 'lbfgs'}
    >>> run.get_metrics()
    {'test accuracy': 0.8393039918116684}
    >>> run.get_models()
    {'model': '../output/census/logreg.gz'}


Overview
========

Everything begins with the :py:mod:`~verta.modeldbclient.ModelDBClient`:

.. code-block:: python

    from verta import ModelDBClient

    client = ModelDBClient(HOST, PORT)

``HOST`` and ``PORT`` point the client to your ModelDB backend instance.

|

Once a client is instantiated and a connection is established, you can create ModelDB entities to
organize your work:

.. code-block:: python

    >>> proj = client.set_project("MNIST Multiclassification")
    created new Project: MNIST Multiclassification
    >>> expt = client.set_experiment("Fully-Connected Neural Network")
    created new Experiment: Fully-Connected Neural Network
    >>> run = client.set_experiment_run("256 Hidden Nodes")

| A *project* is a goal.
| An *experiment* is a strategy for that goal.
| An *experiment run* is an execution of that strategy.

|

You can start logging metadata:

.. code-block:: python

    >>> run.log_hyperparameter("hidden_size", 256)

...and then get it back:

.. code-block:: python

    >>> run.get_hyperparameter("hidden_size")
    256

|

You can find logged runs later:

.. code-block:: python

    >>> proj = client.set_project("MNIST Multiclassification")
    set existing Project: MNIST Multiclassification
    >>> expt = client.set_experiment("Fully-Connected Neural Network")
    set existing Experiment: Fully-Connected Neural Network
    >>> runs = expt.find("hyperparameters.hidden_size")
    >>> runs
    <ExperimentRuns containing 1 runs>

...along with their metadata:

.. code-block:: python

    >>> runs[0].get_hyperparameter("hidden_size")
    256


.. toctree::
    :hidden:
    :maxdepth: 2

    user/quickstart
    api/verta
