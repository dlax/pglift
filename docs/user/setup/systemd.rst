Systemd
=======

.. _systemd_install:

Installation
------------

To operate pglift with systemd, set the ``service_manager`` and ``scheduler``
settings to ``systemd``:

.. code-block:: yaml
   :caption: File ~/.config/pglift/settings.yaml

    service_manager: systemd
    scheduler: systemd

Then use the ``site-configure`` command to install systemd unit templates:

.. code-block:: console

    (.venv) $ pglift site-configure install
    INFO     installing systemd template unit for PostgreSQL
    INFO     installed pglift-postgresql@.service systemd unit at
             ~/.local/share/systemd/user/pglift-postgresql@.service
    INFO     installing systemd template unit for Prometheus postgres_exporter
    INFO     installed pglift-postgres_exporter@.service systemd unit at
             ~/.local/share/systemd/user/pglift-postgres_exporter@.service
    INFO     installing systemd template unit and timer for PostgreSQL backups
    INFO     installed pglift-backup@.service systemd unit at
             ~/.local/share/systemd/user/pglift-backup@.service
    INFO     installed pglift-backup@.timer systemd unit at
             ~/.local/share/systemd/user/pglift-backup@.timer

.. note::
   Use ``pglift site-configure uninstall`` to uninstall those templates.

How it works
------------

By default, systemd is used in `user` mode, by running ``systemctl --user``
commands. This way, the operator can install systemd units in their home
directory (typically in ``$HOME/.local/share/systemd/user``).

Several services are set up at instance creation; these can be listed as
follows for an instance with ``13-main`` identifier:

::

    $ systemctl --user list-units "*13-main*"
      UNIT                                     LOAD   ACTIVE SUB     DESCRIPTION
      pglift-postgres_exporter@13-main.service loaded active running Prometheus exporter for PostgreSQL 13-main database server metrics
      pglift-postgresql@13-main.service        loaded active running PostgreSQL 13-main database server
      pglift-backup@13-main.timer              loaded active waiting Backup 13-main PostgreSQL database instance
    $ systemctl --user list-timers "*13-main*"
    NEXT                         LEFT     LAST                         PASSED       UNIT                            ACTIVATES
    Sat 2021-08-07 00:00:00 CEST 10h left Fri 2021-08-06 12:21:07 CEST 1h 25min ago postgresql-backup@13-main.timer pglift-backup@13-main.service

Overriding
----------

``systemd.service`` and ``systemd.timer`` shipped with pglift may be overridden
using standard methods, as described in `systemd.unit(5)`_.

Here is how to obtain the definition of built-in units (in `user` mode here):

::

    $ systemctl --user list-unit-files pglift-\*
    UNIT FILE                         STATE    VENDOR PRESET
    pglift-backup@.service            static   -
    pglift-postgres_exporter@.service indirect enabled
    pglift-postgresql@.service        indirect enabled
    pglift-backup@.timer              indirect enabled

::

    $ systemctl --user cat pglift-postgresql@.service
    [Unit]
    Description=PostgreSQL %i database server
    After=network.target

    [Service]
    Type=forking


    # Disable OOM kill on the postmaster
    OOMScoreAdjust=-1000
    Environment=PG_OOM_ADJUST_FILE=/proc/self/oom_score_adj
    Environment=PG_OOM_ADJUST_VALUE=0


    ExecStart=/home/denis/src/dalibo/pglift/.venv/bin/python3 -m pglift.postgres %i
    ExecReload=/bin/kill -HUP $MAINPID

    PIDFile=/home/denis/.local/share/pglift/run/postgresql/postgresql-%i.pid

    [Install]
    WantedBy=multi-user.target


.. _`systemd.unit(5)`: https://www.freedesktop.org/software/systemd/man/systemd.unit.html

`system` mode
-------------

Operating pglift with systemd in system mode (i.e. through ``systemctl
--system`` commands) is possible with a few configuration and installation
steps.

First assume we're working in the ``/srv/pglift`` prefix directory, where all
instances data and configuration would live, and set ownership to the current
user:

.. code-block:: console

    $ sudo mkdir /srv/pglift
    $ sudo chown -R $(whoami): /srv/pglift

A typical site settings file would contain:

.. code-block:: console

    $ cat > config.json << EOF
    {
        "service_manager": "systemd",
        "systemd": {
            "unit_path": "/run/systemd/system",
            "user": false,
            "sudo": true
        },
        "sysuser": ["$USER", "$USER"],
        "prefix": "/srv/pglift"
    }
    EOF

- ``systemd`` is set a service manager,
- ``systemd`` is configured to have its unit files in ``/run/systemd/system``,
- the ``systemd.user`` setting is unset (meaning ``--system`` option will be
  passed to ``systemctl``),
- the ``systemd.sudo`` setting can optionnally be set in order to invoke
  ``systemctl`` command with ``sudo``,
- a ``sysuser`` (user name, group name) is set to define the system user
  operating PostgreSQL,
- the global ``prefix`` is set to previously create directory.

Next the site needs to be configured by running:

.. code-block:: console

    $ sudo env SETTINGS=$(pwd)/config.json \
        pglift site-configure install --settings=$(pwd)/config.json

(this may be done at package installation step, if installed from a
distribution package).

Finally, operations are performed as usual but using configured ``sysuser``,
e.g.:

.. code-block:: console

    $ env SETTINGS=$(pwd)/config.json \
        pglift instance create --port=5455 main
