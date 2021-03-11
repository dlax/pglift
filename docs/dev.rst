.. highlight:: console

.. _devenv:

Developement environment
========================

Setup
-----

Install the project in a Python3 virtualenv:

::

    $ python3 -m venv .venv
    $ .venv/bin/activate
    (.venv) $ pip install -e .[dev,test]

Running tests
-------------

The test suite can be run either either directly:

::

    (.venv) $ pytest

or through ``tox``:

::

    $ tox [-e tests]
