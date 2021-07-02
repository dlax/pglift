Command Line Interface
======================

.. highlight:: console

pglift provides a CLI that can be used as follows:

::

    (.venv) $ pglift --help
    Usage: pglift [OPTIONS] COMMAND [ARGS]...

    Deploy production-ready instances of PostgreSQL

    Options:
      ...

    Commands:
      ...

For example, you can describe an instance using the following command:

::

    (.venv) $ pglift instance describe myinstance

The following syntax is also valid:

::

    (.venv) $ python -m pglift instance describe myinstance
