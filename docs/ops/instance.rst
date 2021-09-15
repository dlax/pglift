Instance operations
===================

Command line interface
----------------------

The ``pglift instance`` command line entry point exposes commands to
manage the life-cycle of instances. This includes initialization,
modification, deletion as well as status management (start, stop, restart).

.. code-block:: console

    $ pglift instance --help
    Usage: pglift instance [OPTIONS] COMMAND [ARGS]...

      Manipulate instances

    Options:
      --help  Show this message and exit.

    Commands:
      alter       Alter a PostgreSQL instance
      apply       Apply manifest as a PostgreSQL instance
      backup      Back up a PostgreSQL instance
      describe    Describe a PostgreSQL instance
      drop        Drop a PostgreSQL instance
      init        Initialize a PostgreSQL instance
      list        List the available instances
      privileges  List default privileges on instance.
      reload      Reload a PostgreSQL instance
      restart     Restart a PostgreSQL instance
      restore     Restore a PostgreSQL instance
      schema      Print the JSON schema of PostgreSQL instance model
      shell       Open a PostgreSQL interactive shell on a running instance.
      start       Start a PostgreSQL instance
      status      Check the status of a PostgreSQL instance.
      stop        Stop a PostgreSQL instance


Ansible module
--------------

The ``instance`` module within ``dalibo.pglift`` collection is the main entry
point for instance management through Ansible.

Example task:

.. code-block:: yaml

    tasks:
      - name: my instance
        dalibo.pglift.instance:
          name: myapp
          port: 5455
          ssl: true
          configuration:
            shared_buffers: 1GB
          prometheus_port: 9182

Python API
----------

.. currentmodule:: pglift.instance

Module :mod:`pglift.instance` exposes the following API to manipulate
instances:

.. autofunction:: apply
.. autofunction:: describe
.. autofunction:: list
.. autofunction:: drop
.. autofunction:: init
.. autofunction:: configure
.. autofunction:: start
.. autofunction:: stop
.. autofunction:: restart
.. autofunction:: reload
.. autofunction:: status
.. autofunction:: running
.. autofunction:: promote
