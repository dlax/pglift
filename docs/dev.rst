.. highlight:: console

.. _devenv:

Contributing
------------

Setup
~~~~~

Clone the git repository:

::

    $ git clone https://gitlab.com/dalibo/pglift.git
    $ cd pglift

Then, create a Python3 virtualenv and install the project:

::

    $ python3 -m venv .venv
    $ . .venv/bin/activate
    (.venv) $ pip install -e ".[dev,test]"

Running tests
~~~~~~~~~~~~~

The test suite can be run either either directly:

::

    (.venv) $ pytest

or through ``tox``:

::

    $ tox [-e tests]

By default, tests will not use systemd as a service manager / scheduler. In
order to run tests with systemd, pass the ``--systemd`` option to pytest
command.

The test suite is quite extensive and can take long to run, especially if
several PostgreSQL versions are available on the system. It is split into
*functional* tests and more *unit* ones, the former require a real PostgreSQL
instance (which will be set up automatically) while the latter do not.

When working on a simple fix or changes that would be covered by
non-functional tests, one can run the following part of the test suite
quickly:

::

    (.venv) $ pytest src tests/unit


Pre-commit hooks
~~~~~~~~~~~~~~~~

Some checks (linting, typing, syntax checking, …) can be done for you
before git commits.

You just need to install the pre-commit hooks:

::

    (.venv) $ pre-commit install

Working on documentation
~~~~~~~~~~~~~~~~~~~~~~~~

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

Contributing changes
~~~~~~~~~~~~~~~~~~~~

* Make sure that lint, typing checks pass as well as at least unit tests.
* When committing changes with git, write one commit per logical change and
  try to follow pre-existing style and write a meaningful commit message (see
  https://commit.style/ for a quick guide).

Release workflow
~~~~~~~~~~~~~~~~

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

    $ git push --follow-tags

* Finally get your PyPI API token and run:

  .. code-block:: bash

    $ env PYPI_TOKEN=pypi-xxx tox -e release

  to build and upload the Python package to `PyPI
  <https://pypi.org/project/pglift>`_.
