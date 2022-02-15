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
    name       version    port  path                                          status
    -------  ---------  ------  --------------------------------------------  -----------
    local           13    7892  .../.local/share/pglift/srv/pgsql/13/local    running
    standby         13    7893  .../.local/share/pglift/srv/pgsql/13/standby  not_running
    main            13    5455  .../.local/share/pglift/srv/pgsql/13/main     running

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

    $ pglift instance describe main
    name: main
    version: '13'
    port: 5456
    state: started
    ssl: false
    configuration: {}
    standby: null
    prometheus:
      port: 9187

.. note::

    PostgreSQL instance configuration can be managed using the ``pgconf``
    command, as described in more details in :ref:`the dedicated section
    <pgconf>`. A few quick examples:
    ::

        $ pglift pgconf show main log_connections
        log_connections = off
        $ pglift pgconf set main log_connections=on
        log_connections: off -> on

Adding and manipulating instance objects:

::

    $ pglift role create 13/main dba --password --login
    Password:
    Repeat for confirmation:

::

    $ pglift role describe 13/main dba
    name: dba
    password: '**********'
    pgpass: false
    inherit: true
    login: true
    connection_limit: null
    validity: null
    in_roles: []

::

    $ pglift role alter 13/main dba --connection-limit=10 --in-role=pg_monitor --inherit

::

    $ pglift role describe 13/main dba
    name: dba
    password: '**********'
    pgpass: false
    inherit: true
    login: true
    connection_limit: 10
    validity: null
    in_roles:
    - pg_monitor

::

    $ pglift database create 13/main myapp

::

    $ pglift database alter 13/main myapp --owner dba

::

    $ pglift database describe 13/main myapp
    name: myapp
    owner: dba

::

    $ pglift database list 13/main
    name       owner     encoding    collation    ctype    acls                                         size  description                                 tablespace    tablespace      tablespace
                                                                                                                                                          name          location              size
    ---------  --------  ----------  -----------  -------  ----------------------------------------  -------  ------------------------------------------  ------------  ------------  ------------
    myapp      postgres  UTF8        C            C                                                  8167939                                              pg_default                      41011771
    postgres   postgres  UTF8        C            C                                                  8319535  default administrative connection database  pg_default                      41011771
    template1  postgres  UTF8        C            C        ['=c/postgres', 'postgres=CTc/postgres']  8167939  default template for new databases          pg_default                      41011771

::

    $ pglift database drop 13/main myapp
