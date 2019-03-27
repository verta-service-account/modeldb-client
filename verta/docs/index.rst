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


.. toctree::
    :hidden:
    :titlesonly:

    guides/guides
    reference/reference
