Integrate **ModelDB** with Workflows
====================================

Everything begins with the :py:mod:`~verta.modeldbclient.ModelDBClient`:

.. code-block:: python

    >>> from verta import ModelDBClient
    >>> client = ModelDBClient(HOST, PORT)
    connection successfully established

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
