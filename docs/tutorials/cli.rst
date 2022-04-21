Using the Command Line Interface
================================

.. highlight:: console

pglift provides a CLI that can be used as follows:

::

    $ pglift --help
    Usage: pglift [OPTIONS] COMMAND [ARGS]...

    Deploy production-ready instances of PostgreSQL

    Options:
      ...

    Commands:
      ...

There are several entry points corresponding to main objects handled by
pglift: instances, roles, databases, pgconf, etc. Each entry point has its own help:

::

    $ pglift instance create --help
    Usage: pglift instance create [OPTIONS] NAME

      Initialize a PostgreSQL instance

    Options:
      --version VERSION            Postgresql version.
      --port PORT                  Tcp port the postgresql instance will be
                                   listening to.
      --state [started|stopped]    Runtime state.
      --surole-password TEXT       Super-user role password.
      --replication-password TEXT  Replication role password.
      --standby-for FOR            Dsn of primary for streaming replication.
      --standby-slot SLOT          Replication slot name.
      --prometheus-port PORT       Tcp port for the web interface and telemetry of
                                   prometheus.
      --help                       Show this message and exit.

Most top-level commands like ``database`` or ``role`` operate on a particular
instance which needs to be specified through ``-i``/``--instance`` option;
the option is *required* unless there is only one existing instance.

Creating an instance:

::

    $ pglift instance create main --port=5455

a standby instance can also be created by passing the
``--standby-for=<primary dsn>`` option to ``instance create`` command, see
:doc:`/howto/standby-setup` for dedicated documentation.

The instance actually consists of a PostgreSQL instance with a backup service (pgbackrest)
and a monitoring service (Prometheus postgres_exporter) set up.

Listing instances:

::

    $ pglift instance list
     ─────────────────────────────────────────────────────────────────────────────────────────
    │ name      │ version │ port │ path                                         │ status      │
    └───────────┴─────────┴──────┴──────────────────────────────────────────────┴─────────────┘
    │ local     │ 13      │ 7892 │ .../.local/share/pglift/srv/pgsql/13/local   │ running     │
    │ standby   │ 13      │ 7893 │ .../.local/share/pglift/srv/pgsql/13/standby │ not_running │
    │ main      │ 13      │ 5455 │ .../.local/share/pglift/srv/pgsql/13/main    │ running     │
    └───────────┴─────────┴──────┴──────────────────────────────────────────────┴─────────────┘

Altering an instance:

::

    $ pglift instance alter main --port=5456
    INFO     configuring PostgreSQL instance
    INFO     setting up Prometheus postgres_exporter service
    INFO     setting up pgBackRest
    INFO     updating entry for 'postgres' in ~/.pgpass (port changed)
    INFO     configuring PostgreSQL authentication
    > Instance needs to be restarted; restart now? [y/n]: y
    INFO     restarting instance 14/main

Getting instance information:

::

    $ pglift instance get main --json
    {
      "name": main
      "version": '13'
      "port": 5456
      "state": started
      "ssl": false
      "configuration": {}
      "standby": null
      "prometheus": {
        "port": 9187
      }
    }

.. note::

    PostgreSQL instance configuration can be managed using the ``pgconf``
    command, as described in more details in :ref:`the dedicated section
    <pgconf>`. A few quick examples:
    ::

        $ pglift pgconf -i main show log_connections
        log_connections = off
        $ pglift pgconf -i main set log_connections=on
        log_connections: off -> on

Adding and manipulating instance objects:

::

    $ pglift role -i 13/main create dba --password --login
    Password:
    Repeat for confirmation:

::

    $ pglift role -i 13/main get dba
    name  password    pgpass  inherit  login  superuser  replication  connection_limit  validity  in_roles
    dba   **********  False   True     True   False      False

::

    $ pglift role -i 13/main alter dba --connection-limit=10 --in-role=pg_monitor --inherit

::

    $ pglift role -i 13/main get dba --json
    {
      "name": "dba",
      "password": "**********",
      "pgpass": false,
      "inherit": true,
      "login": true,
      "superuser": false,
      "replication": false,
      "connection_limit": 10,
      "validity": null,
      "in_roles": [
        "pg_monitor"
      ]
    }

::

    $ pglift database -i 13/main create myapp

::

    $ pglift database -i 13/main alter myapp --owner dba

::

    $ pglift database -i 13/main get myapp
    name   owner  settings
    myapp  dba

::

    $ pglift database -i 13/main list
     ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    │ name      │ owner    │ encoding │ collation │ ctype   │ acls                  │ size   │ description               │ tablespace       │
    │           │          │          │           │         │                       │        │                           │                  │
    └───────────┴──────────┴──────────┴───────────┴─────────┴───────────────────────┴────────┴───────────────────────────┴──────────────────┘
    │ myapp     │ postgres │ UTF8     │ C         │ C       │                       │ 8.2MiB │                           │ name: pg_default │
    │           │          │          │           │         │                       │        │                           │ location:        │
    │           │          │          │           │         │                       │        │                           │ size: 41.0MiB    │
    │ postgres  │ postgres │ UTF8     │ C         │ C       │                       │ 8.3MiB │ default administrative    │ name: pg_default │
    │           │          │          │           │         │                       │        │ connection database       │ location:        │
    │           │          │          │           │         │                       │        │                           │ size: 41.0MiB    │
    │ template1 │ postgres │ UTF8     │ C         │ C       │ =c/postgres,          │ 8.2MiB │ default template for new  │ name: pg_default │
    │           │          │          │           │         │ postgres=CTc/postgres │        │ databases                 │ location:        │
    │           │          │          │           │         │                       │        │                           │ size: 41.0MiB    │
    └───────────┴──────────┴──────────┴───────────┴─────────┴───────────────────────┴────────┴───────────────────────────┴──────────────────┘

::

    $ pglift database -i 13/main drop myapp
