Getting Started
===============

0–60 in 60 seconds with **Verta**:

1. Install the **Verta** Python package.

  .. code-block:: console

    $ pip3 install verta

  Note that Verta currently officially supports Python 3.5–3.7. For more information, read the
  `installation guide <package.html>`_.

2. Integrate the Verta package into your workflow.

  a. Connect to the ModelDB server:

    .. code-block:: python

        from verta import ModelDBClient
        client = ModelDBClient(HOST, PORT)

    For more information, read the `back end guide <back-end.html>`_.

  b. Log things that matter to you:

    .. code-block:: python

        proj = client.set_project("Fraud Detection")
        expt = client.set_experiment("Recurrent Neural Net")

    .. code-block:: python

        run = client.set_experiment_run("Two-layer dropout LSTM")
        ...
        run.log_hyperparameter("num_layers", 2)
        run.log_hyperparameter("hidden_size", 512)
        run.log_hyperparameter("dropout", 0.5)
        run.log_metric("accuracy", 0.95)

    For more information, read the `workflow guide <workflows.html>`_ or check the `API reference
    <../reference/api.html>`_.

3. Now that we've logged a few runs, head to the `Verta Web App <https://example.com>`_ to view them!

  For more information, read the `front end guide <front-end.html>`_.


.. toctree::
    :hidden:

    Client Package <package>
    Back End <back-end>
    Workflows <workflows>
    Front End <front-end>
