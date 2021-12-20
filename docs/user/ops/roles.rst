Roles operations
================

Command line interface
----------------------

The ``pglift role`` command line entry point exposes commands to
manage PostgreSQL roles of an instance.

.. code-block:: console

    $ pglift role --help
    Usage: pglift role [OPTIONS] COMMAND [ARGS]...

      Manipulate roles

    Options:
      --help  Show this message and exit.

    Commands:
      alter       Alter a role in a PostgreSQL instance
      apply       Apply manifest as a role
      create      Create a role in a PostgreSQL instance
      describe    Describe a role
      drop        Drop a role
      privileges  List default privileges of a role.
      schema      Print the JSON schema of role model

Ansible module
--------------

The ``role`` module within ``dalibo.pglift`` collection is the main entry
point for PostgreSQL roles management through Ansible.

Example task:

.. code-block:: yaml

    tasks:
      - name: my role
        dalibo.pglift.role:
          instance: myinstance
          name: dba
          pgpass: true
          login: true
          connection_limit: 10
          validity: '2025-01-01T00:00'
          in_roles:
            - pg_read_all_stats
            - pg_signal_backend
