Install the Client
==================

**Verta** officially supports Python 3.5–3.7, with 2.7 and more on the way!

To check your version of Python:

.. code-block:: console

    $ python3 -V

Or if you don't yet have Python 3, you can install it:

- on Windows by first installing `Chocolatey <https://chocolatey.org/>`_ and then running:

  .. code-block:: console

      $ choco install python3

- on macOS by first installing `Homebrew <https://brew.sh/>`_ and then running:

  .. code-block:: console

      $ brew install python3

- on Linux by running:

  .. code-block:: console

      $ sudo apt install python3


via pip
-------

It's recommended to first create and activate a virtual environment:

.. code-block:: console

    $ python3 -m venv venv
    $ source venv/bin/activate

Then, install **Verta**:

.. code-block:: console

    (venv) $ pip3 install verta


via conda
---------

It's recommended to first create and activate a virtual environment

.. code-block:: console

    $ conda create -n venv python=3
    $ conda activate venv

Then, install **Verta**:

.. code-block:: console

    (venv) $ conda install verta -c conda-forge
