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

    $ pglift instance restore -l local
    label                                     size    repo_size  datetime                   type    databases
    ---------------------------------  -----------  -----------  -------------------------  ------  -----------------------------------
    20210914-103658F                   5.77217e+07   7.1039e+06  2021-09-14 10:36:58+02:00  full    db, myapp, mydb, otherapp, postgres
    20210914-103154F_20210914-103644D  5.77217e+07   7.1039e+06  2021-09-14 10:36:44+02:00  diff    db, myapp, mydb, otherapp, postgres
    20210914-103154F_20210914-103221D  5.77217e+07   7.1039e+06  2021-09-14 10:32:21+02:00  diff    db, myapp, mydb, otherapp, postgres
    20210914-103154F                   5.77217e+07   7.1039e+06  2021-09-14 10:31:54+02:00  full    db, myapp, mydb, otherapp, postgres

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
