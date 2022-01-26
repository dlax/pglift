Settings
========

The details of most operations can be configured by defining the *settings* of
an installation. These settings are usually gathered in a configuration file,
in YAML format, and some can be overridden from environment variables.

A typical settings document looks like:

.. code-block:: json

    {
      "postgresql": {
        "bindir": "/usr/lib/postgresql/{version}/bin",
        "versions": {
          "13": {
            "bindir": "/usr/lib/postgresql/13/bin"
          },
          "11": {
            "bindir": "/usr/lib/postgresql/11/bin"
          }
        },
        "root": "/srv/pgsql",
        "initdb": {
          "locale": "fr_FR.UTF8",
          "data_checksums": true
        },
        "auth": {
          "local": "trust",
          "host": "scram-sha-256"
        },
        "surole": "postgres",
        "datadir": "data",
        "waldir": "wal",
        "pid_directory": "/run/postgresql"
      },
      "pgbackrest": {
        "execpath": "/usr/bin/pgbackrest",
        "configpath": "/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf",
        "directory": "/srv/pgbackrest/{instance.version}-{instance.name}",
        "logpath": "/srv/pgbackrest/{instance.version}-{instance.name}/logs"
      },
      "prometheus": {
        "execpath": "/usr/bin/prometheus-postgres-exporter",
        "configpath": "/etc/prometheus/postgres_exporter-{instance.version}-{instance.name}.conf",
        "queriespath": "/etc/prometheus/postgres_exporter_queries-{instance.version}-{instance.name}.yaml",
        "port": 9187
      },
      "service_manager": "systemd",
      "scheduler": "systemd",
      "prefix": "/"
    }

Apart from ``postgresql``, most top-level keys correspond to *components* of
instances and their value thus defines how these components are installed,
configured, run. Some other top-level keys correspond to cross-service
settings defining, e.g., how scheduled tasks are run or which service manager
is used.

To view current settings, run:

::

    $ pglift site-settings


Site (or installation) settings are looked up for at the following locations:

- ``$XDG_CONFIG_DIR/pglift/settings.yaml`` [#xdgconfighome]_, then,
- ``/etc/pglift/settings.yaml``.

Once one of these files is found, processing stops.

.. note::

    To temporarily override installed settings, the ``SETTINGS`` environment
    variable can be used. It accepts either a JSON-dumped value or a file path,
    prepended with ``@``:

    ::

        $ SETTINGS='{"postgresql": {"root": "/srv/postgres"}}'
        $ SETTINGS=@/path/to/config.json

    Alternatively, an *hidden* ``--settings`` option is available:

    ::

        $ pglift --settings=/path/to/config.yaml <command>

.. [#xdgconfighome]
   Where ``$XDG_CONFIG_DIR`` would be ``$HOME/.config`` unless configured
   differently.
