.. highlight:: console

.. _devenv:

Development environment
=======================

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

Pre-commit hooks
----------------

Some checks (linting, typing, syntax checking, …) can be done for you
before git commits.

You just need to install the pre-commit hooks:

::

    (.venv) $ pre-commit install
