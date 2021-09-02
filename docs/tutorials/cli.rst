Command Line Interface
======================

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
pglift: instances, roles, databases, etc. Each entry point has its own help:

::

    $ pglift instance init --help
    Usage: pglift instance init [OPTIONS] NAME

      Initialize a PostgreSQL instance

    Options:
      --version VERSION          Postgresql version.
      --port PORT                Tcp port the postgresql instance will be
                                 listening to.
      --state [started|stopped]  Runtime state.
      --standby-for FOR          Dsn of primary for streaming replication.
      --standby-slot SLOT        Replication slot name.
      --prometheus-port PORT     Tcp port for the web interface and telemetry of
                                 prometheus.
      --help                     Show this message and exit.

Creating an instance:

::

    $ pglift instance init main --port=5455

a standby instance can also be created by passing the
``--standby-for=<primary dsn>`` option to ``instance init`` command.

The instance actually consists of a PostgreSQL instance with a backup service (pgbackrest)
and a monitoring service (Prometheus postgres_exporter) set up. By default,
everything runs through systemd user services:

::

    $ systemctl --user list-units "*13-main*"
      UNIT                              LOAD   ACTIVE SUB     DESCRIPTION
      postgres_exporter@13-main.service loaded active running Prometheus exporter for PostgreSQL 13-main database server metrics
      postgresql@13-main.service        loaded active running PostgreSQL 13-main database server
      postgresql-backup@13-main.timer   loaded active waiting Backup 13-main PostgreSQL database instance
    $ systemctl --user list-timers "*13-main*"
    NEXT                         LEFT     LAST                         PASSED       UNIT                            ACTIVATES
    Sat 2021-08-07 00:00:00 CEST 10h left Fri 2021-08-06 12:21:07 CEST 1h 25min ago postgresql-backup@13-main.timer postgresql-backup@13-main.service




Listing instances:

::

    $ pglift instance list
    Name       Version    Port  Path                                          Status
    -------  ---------  ------  --------------------------------------------  -----------
    local           13    7892  .../.local/share/pglift/srv/pgsql/13/local    running
    standby         13    7893  .../.local/share/pglift/srv/pgsql/13/standby  not_running
    main            13    5455  .../.local/share/pglift/srv/pgsql/13/main     running

Altering an instance:

::

    $ pglift instance alter main --port=5456
    $ pglift instance restart main

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

Adding and manipulating instance objects:

::

    $ pglift role create 13/main dba --password --login
    Password:
    Repeat for confirmation:
    $ pglift role describe 13/main dba
    name: dba
    password: '**********'
    pgpass: false
    inherit: true
    login: true
    connection_limit: null
    validity: null
    in_roles: []
    $ pglift role alter 13/main dba --connection-limit=10 --in-role=pg_monitor --inherit
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
    $ pglift database alter 13/main myapp --owner dba
    $ pglift database describe 13/main myapp
    name: myapp
    owner: dba
    $ pglift database drop 13/main myapp