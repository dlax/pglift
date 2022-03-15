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
    (.venv) $ pip install --upgrade pip
    (.venv) $ pip install -e ".[dev,test]"

Though not required, tox_ can be used to run all checks (lint, tests, etc.)
needed in development environment.

.. _tox: https://tox.wiki/

Linting, formatting, type-checking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The project uses flake8_ for linting, black_ and isort_ for formatting and
mypy_ for type-checking. The packaging is checked with `check-manifest`_.

All these checks can be run with ``tox -e lint`` or individually.

.. _flake8: https://flake8.pycqa.org/
.. _black: https://black.readthedocs.io/
.. _isort: https://pycqa.github.io/isort/
.. _mypy: https://mypy.readthedocs.io/
.. _`check-manifest`: https://pypi.org/project/check-manifest/

Running tests
~~~~~~~~~~~~~

The test suite can be run either either directly:

::

    (.venv) $ pytest

or through ``tox``:

::

    $ tox [-e tests-unit|tests-func]

The test suite is quite extensive and can take long to run. It is split into
*functional* tests and more *unit* ones, the former require a real PostgreSQL
instance (which will be set up automatically) while the latter do not. Each
test suite gets a dedicated tox environment: ``tests-unit`` and
``tests-func``.

When working on a simple fix or change that would be covered by non-functional
tests, one can run the following part of the test suite quickly:

::

    (.venv) $ pytest src tests/unit

or through ``tox``:

::

    $ tox -e tests-unit

By default, functional tests will not use systemd as a service manager /
scheduler. In order to run tests with systemd, pass the ``--systemd`` option
to pytest command.

Still in functional tests, the PostgreSQL environment would be guessed by
inspecting the system to look for PostgreSQL binaries for the most recent
version available. If multiple versions of PostgreSQL are available, a
specific version can be selected by passing ``--pg-version=<version>`` option
to the ``pytest`` command.

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

* Push the tag to the main (upstream) repository:

  .. code-block:: bash

    $ git push --follow-tags

* Finally, the CI will build and upload the Python package to `PyPI
  <https://pypi.org/project/pglift>`_.
