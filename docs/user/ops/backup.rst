Backup
======

Command line interface
----------------------

The ``pglift instance`` command line entry point exposes ``backup`` and
``restore`` commands to respectively perform instance-level backup and
restoration using selected PITR tool, currently pgBackRest_.

.. code-block:: console

    $ pglift instance backup --help
    Usage: pglift instance backup [OPTIONS] NAME [VERSION]

      Back up a PostgreSQL instance

    Options:
      --type [full|incr|diff]  Backup type
      --purge                  Purge old backups
      --help                   Show this message and exit.
    $ pglift instance restore --help
    Usage: pglift instance restore [OPTIONS] NAME [VERSION]

      Restore a PostgreSQL instance

    Options:
      -l, --list                      Only list available backups
      --label TEXT                    Label of backup to restore
      --date [%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]
                                      Date of backup to restore
      --help                          Show this message and exit.

The ``restore`` command can be used to list available backups:

.. code-block:: console

    $ pglift instance restore -l main
                                                         Available backups for instance 14/main
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
    ┃ label                             ┃ size    ┃ repo_size ┃ date_start                ┃ date_stop                 ┃ type ┃ databases           ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
    │ 20220323-152037F                  │ 41.7MiB │ 5.3MiB    │ 2022-03-23 15:20:37+01:00 │ 2022-03-23 15:20:46+01:00 │ full │ db, myapp, postgres │
    │ 20220323-151809F_20220323-151849D │ 25.3MiB │ 3.2MiB    │ 2022-03-23 15:18:49+01:00 │ 2022-03-23 15:18:50+01:00 │ diff │ postgres            │
    │ 20220323-151809F                  │ 25.3MiB │ 3.2MiB    │ 2022-03-23 15:18:09+01:00 │ 2022-03-23 15:18:15+01:00 │ full │ postgres            │
    └───────────────────────────────────┴─────────┴───────────┴───────────────────────────┴───────────────────────────┴──────┴─────────────────────┘

Scheduled backups
-----------------

At instance creation, when `systemd` is used as a `scheduler`, a timer for
periodic backup is installed:

.. code-block:: console

    $ systemctl --user list-timers
    NEXT                         LEFT     LAST                         PASSED       UNIT                            ACTIVATES
    Thu 2021-09-16 00:00:00 CEST 12h left Wed 2021-09-15 08:15:58 CEST 3h 23min ago postgresql-backup@13-main.timer postgresql-backup@13-main.service

    1 timers listed.
    $ systemctl --user cat postgresql-backup@13-main.service
    [Unit]
    Description=Backup %i PostgreSQL database instance
    After=postgresql@%i.service

    [Service]
    Type=oneshot

    ExecStart=/usr/bin/python3 -m pglift.backup %i


.. _pgBackRest: https://pgbackrest.org/
