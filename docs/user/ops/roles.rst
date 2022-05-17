Roles operations
================

Command line interface
----------------------

The ``pglift role`` command line entry point exposes commands to
manage PostgreSQL roles of an instance.

.. code-block:: console

    $ pglift role --help
    Usage: pglift role [OPTIONS] COMMAND [ARGS]...

      Manage roles.

    Options:
      -i, --instance <version>/<name>
                                      Instance identifier; the <version>/ prefix
                                      may be omitted if there's only one instance
                                      matching <name>. Required if there is more
                                      than one instance on system.
      --schema                        Print the JSON schema of role model and
                                      exit.
      --help                          Show this message and exit.

    Commands:
      alter       Alter a role in a PostgreSQL instance
      create      Create a role in a PostgreSQL instance
      drop        Drop a role
      get         Get the description of a role
      privileges  List default privileges of a role.

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

It's also possible to use the ``instance`` module within ``dalibo.pglift``
collection as the main entry point to manage your PostgreSQL ``role``. Some
examples about managing roles directly with the Ansible instance module are
available under the :ref:`dedicated documentation <instance-module>`.
