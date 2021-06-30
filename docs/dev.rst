.. highlight:: console

.. _devenv:

Development environment
=======================

Setup
-----

Install the project in a Python3 virtualenv:

::

    $ python3 -m venv .venv
    $ . .venv/bin/activate
    (.venv) $ pip install -e ".[dev,test]"

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

Working on documentation
------------------------

Building the documentation requires a few more dependencies:

::

    (.venv) $ pip install -e .[docs] sphinx-autobuild

Then, run:

::

    (.venv) $ make -C docs html

to actually build the documentation and finally open
``docs/_build/html/index.html`` to browse the result.

Alternatively, keep the following command running:

::

    (.venv) $ make -C docs serve

to get the documentation rebuilt and along with a live-reloaded Web browser
(the reason for ``sphinx-autobuild`` dependency above).

Release workflow
----------------

* Create an *annotated* git tag following the ``v<MAJOR>.<MINOR>.<PATCH>``
  pattern. For instance:

  .. code-block:: bash

    $ git tag v0.1.0 -a [-s] -m 'pglift v0.1.0' --edit

  then edit the tag message to include a changelog since latest tag.

  That changelog can be obtained using:

  .. code-block:: bash

    $ git log $(git describe --tags --abbrev=0).. --format=%s

* Push the tag:

  .. code-block:: bash

    $ git push --tags

* Finally get your PyPI API token and run:

  .. code-block:: bash

    $ env PYPI_TOKEN=pypi-xxx tox -e release

  to build and upload the Python package to `PyPI
  <https://pypi.org/project/pglift>`_.
