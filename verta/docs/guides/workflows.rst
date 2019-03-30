Integrate with Workflows
========================

We'll walk through an example workflow together so that you can see the ModelDB client in action!


Setup
-----

First, let's install a machine learning library to work with:

.. code-block:: console

    (venv) $ pip3 install sklearn

...then launch the Python interpreter:

.. code-block:: console

    (venv) $ python3
    >>>

|

We begin with the :py:mod:`~verta.modeldbclient.ModelDBClient`:

.. code-block:: python

    >>> from verta import ModelDBClient
    >>> client = ModelDBClient(HOST, PORT)
    connection successfully established

``HOST`` and ``PORT`` point the client to your ModelDB back end instance, which we had set up in `the
previous guide <back-end.html>`_\ . You'll have to replace ``HOST`` and ``PORT`` here with the actual
hostname and port number of your own back end.

|

Once a client is instantiated and a connection is established, you can create ModelDB entities to
organize your work:

.. code-block:: python

    >>> proj = client.set_project("Digit Multiclassification")
    created new Project: Digit Multiclassification
    >>> expt = client.set_experiment("Support Vector Machine")
    created new Experiment: Support Vector Machine
    >>> run = client.set_experiment_run("RBF Kernel")

A *Project* is a goal. We're going to classify multiple handwritten digits.

An *Experiment* is a strategy for that goal. We'll use a support vector machine as our classification
model.

An *Experiment Run* is an execution of that strategy. We'll train a support vector machine using the
radial basis function kernel.

Note that you are not restricted to any naming conventions here. Feel free to use names that you
consider useful and meaningful.

If you'd like, you could also add a description, tags, and attributes:

.. code-block:: python

    >>> # run = client.set_experiment_run("Run 1",
    ... #                                 "SVM w/ RBF kernel",
    ... #                                 ["example"],
    ... #                                 {'architecture': "SVM"})


Run Tracking
------------

scikit-learn has built-in datasets we can use:

.. code-block:: python

    >>> from sklearn.datasets import load_digits
    >>> digits = load_digits()
    >>> X, y = digits.data, digits.target

We also need to define some hyperparameters to specify a configuration for our model:

.. code-block:: python

    >>> hyperparams = {'kernel': "rbf",
    ...                'C': 1e-2,
    ...                'gamma': .2}

Then we can finally train a model on our data:

.. code-block:: python

    >>> from sklearn.svm import SVC
    >>> clf = SVC(**hyperparams).fit(X, y)

To see how well we did, we can calculate our mean accuracy on the entire training set:

.. code-block:: python

    >>> train_acc = clf.score(X, y)
    >>> print(train_acc)
    0.1018363939899833

|

That's not much better than purely guessing! So how do we keep a more permanent record of this abysmal
*Experiment Run*? With ModelDB of course:

.. code-block:: python

    >>> run.log_dataset("train_data", "data/digits.gz", digits)
    >>> run.log_hyperparameters(**hyperparams)
    >>> run.log_model("model", "models/rbf_svc.gz", clf)
    >>> run.log_metric("train_acc", train_acc)

|

But logging doesn't need to occur all at once at the end. Let's do another *Experiment Run* with a
linear kernel—this time interweaving the logging statements with our training process:

.. code-block:: python
    :emphasize-lines: 1,2,4,6,8

    >>> run = client.set_experiment_run("Linear Kernel")
    >>> run.log_dataset("train_data", "data/digits.gz")
    >>> hyperparams['kernel'] = 'linear'
    >>> run.log_hyperparameters(**hyperparams)
    >>> clf = SVC(**hyperparams).fit(X, y)
    >>> run.log_model("model", "models/linear_svc.gz", clf)
    >>> train_acc = clf.score(X, y)
    >>> run.log_metric("train_acc", train_acc)


Querying
--------

Organizing *Experiment Run*\ s under *Experiment*\ s gives us the ability to retrieve them as a group:

.. code-block:: python

    >>> runs = expt.expt_runs
    >>> runs
    <ExperimentRuns containing 2 runs>

...and query them:

.. code-block:: python

    >>> best_run = runs.sort("metrics.train_acc", descending=True)[0]
    >>> best_run.get_metric("train_acc")
    0.9994435169727324

That's pretty good! So which run was this? Definitely not the RBF kernel:

.. code-block:: python

    >>> best_run.name
    'Linear Kernel'


Reproducing
-----------

We can load back the model to see it again for ourselves:

.. code-block:: python

    >>> clf = best_run.get_model("model", load=True)
    >>> clf.score(X, y)
    0.9994435169727324

Or we can retrain the model from scratch as a sanity check:

.. code-block:: python

    >>> clf = SVC(**best_run.get_hyperparameters()).fit(X, y)
    >>> clf.score(X, y)
    0.9994435169727324
