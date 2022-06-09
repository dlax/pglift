Monitoring
==========

Instance monitoring can be handled by Prometheus and/or PoWA.

Prometheus
----------

Prometheus satellite component can be enabled through site settings by
defining a ``prometheus`` key with at least `execpath` field set, e.g.:

.. code-block:: yaml
   :caption: settings.yaml

    prometheus:
      execpath: /usr/bin/prometheus-postgres-exporter

.. note::
   Use ``pglift site-settings --schema`` (possibly piped through ``jq
   .definitions.PrometheusSettings``) to retrieve possible settings for the
   ``prometheus`` section.

.. note::

    `Prometheus postgres_exporter`_ must be **installed** on the system.

A service for `Prometheus postgres_exporter`_ will be deployed at instance
creation.

Command line interface
~~~~~~~~~~~~~~~~~~~~~~

The ``postgres_exporter`` command line entry point exposes commands to start
and stop the service, when bound to a local instance. It also provides
installation commands to setup a postgres_exporter service on a remote host:

.. code-block:: console

    $ pglift postgres_exporter install dbserver "host=dbserver.example.com user=monitoring" 9188 --state=stopped --password
    Password:
    Repeat for confirmation:
    $ pglift postgres_exporter start --foreground dbserver
    INFO[0000] Established new database connection to "dbserver.example.com:5432".  source="postgres_exporter.go:878"
    ...
    $ pglift postgres_exporter uninstall dbserver


Ansible module
~~~~~~~~~~~~~~

The ``postgres_exporter`` module within ``dalibo.pglift`` collection is the
main entry point for managing a `postgres_exporter` service for a non-local
instance through Ansible.

Example task:

.. code-block:: yaml

    tasks:
      - dalibo.pglift.postgres_exporter:
          name: 13-main  # usually a reference to target instance
          dsn: "port=5455 host=dbserver.example.com role=monitoring"
          password: "m0n 1tor"
          port: 9871


temBoard
--------

`pglift` provides support for `temBoard`_.

This component can be enabled through site settings by defining a non-``null``
``temboard`` key, e.g.:

.. code-block:: yaml
   :caption: settings.yaml

    temboard: {}

.. note::

    `temBoard agent` must be **installed** on the system.

The agent is configured automatically at instance creation. It must then be
registered on the `temBoard UI`_ (created outside of `pglift`).

You may find the secret key in temboard's configuration file as defined in
``temboard.configpath`` setting (e.g.
``$prefix/etc/temboard-agent/temboard-agent-14-main.conf``).

Command line interface
~~~~~~~~~~~~~~~~~~~~~~

The ``temboard-agent`` command line entry point exposes command to return
the instance secret key:

.. code-block:: console

    $ pglift temboard-agent -i 14/main secret-key
    123456789

You need this key to `add new instance`_ in `temBoard UI`_.


PoWA
----

In `pglift`, `PoWA`_ is meant to be used in `Remote setup`_ mode (ie. stats
data collected from a remote server).

This component can be enabled through site settings by defining a non-``null``
``powa`` key, e.g.:

.. code-block:: yaml
   :caption: settings.yaml

    powa: {}

.. note::

    `PoWA archivist` (ie. `powa`) and `Stats Extensions` (ie.
    `pg_stat_kcache`, `pg_qualstats`) must be **installed** (via packages) on
    the system.

The extensions for collecting stats for `PoWA`_ are configured and installed
automatically at instance creation.

Once created the instance can be `registered`_ on the PoWA repository (created
outside of `pglift`).



.. _`Prometheus postgres_exporter`: https://github.com/prometheus-community/postgres_exporter
.. _`PoWA`: https://powa.readthedocs.io/en/latest/
.. _`Remote setup`: https://powa.readthedocs.io/en/latest/remote_setup.html
.. _`registered`: https://powa.readthedocs.io/en/latest/components/powa-archivist/configuration.html#powa-register-server
.. _`temBoard`: https://temboard.readthedocs.io/en/latest/
.. _`temBoard UI`: https://temboard.readthedocs.io/en/latest/temboard-howto/
.. _`add new instance`: https://temboard.readthedocs.io/en/latest/temboard-howto/#add-a-new-instance
