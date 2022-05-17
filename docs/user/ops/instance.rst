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

      Manage instances.

    Options:
      --schema  Print the JSON schema of instance model and exit.
      --help    Show this message and exit.

    Commands:
      alter       Alter PostgreSQL INSTANCE
      backup      Back up PostgreSQL INSTANCE
      create      Initialize a PostgreSQL instance
      drop        Drop PostgreSQL INSTANCE
      env         Output environment variables suitable to connect to...
      exec        Execute command in the libpq environment for PostgreSQL...
      get         Get the description of PostgreSQL INSTANCE
      list        List the available instances
      logs        Output INSTANCE logs
      privileges  List default privileges on INSTANCE
      promote     Promote standby PostgreSQL INSTANCE
      reload      Reload PostgreSQL INSTANCE
      restart     Restart PostgreSQL INSTANCE
      restore     Restore PostgreSQL INSTANCE
      start       Start PostgreSQL INSTANCE
      status      Check the status of PostgreSQL INSTANCE.
      stop        Stop PostgreSQL INSTANCE
      upgrade     Upgrade INSTANCE using pg_upgrade

Most commands take an ``INSTANCE`` argument in the form of
``<version>/<name>`` where ``<version>/`` might be omitted. If there is only
one instance on system, the argument is optional.

.. _instance-module:

Ansible module
--------------

The ``instance`` module within ``dalibo.pglift`` collection is the main entry
point for instance management through Ansible. This module also handles roles and
databases objects related to the instance.

Example task (without databases and roles):

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
          roles:
          databases:
            - name: myapp
              owner: dba

Example task (with databases and roles):

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
          roles:
            - name: dba
              pgpass: true
              login: true
              connection_limit: 10
              validity: '2025-01-01T00:00'
              in_roles:
                - pg_read_all_stats
            - name: simple_user
              connection_limit: 10
              validity: '2025-01-01T00:00'
          databases:
            - name: myapp
              owner: dba
            - name: db
              owner: simple_user
