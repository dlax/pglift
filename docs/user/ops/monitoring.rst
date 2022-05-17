Monitoring
==========

Instance monitoring can be handled by Prometheus and/or PoWA.

Prometheus
----------

A service for `Prometheus postgres_exporter`_ can be deployed at instance
creation.

.. note::

    `Prometheus postgres_exporter`_ must be **installed** on the system.
    Prometheus also needs to be **enabled** via the :ref:`site settings
    <settings>`.

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


PoWA
----

In `pglift`, `PoWA`_ is meant to be used in `Remote setup`_ mode.

The extensions for collecting stats for `PoWA`_ are configured and installed
at instance creation.

.. note::

    `PoWA archivist` (ie. `powa`) and `Stats Extensions` (ie.
    `pg_stat_kcache`, `pg_qualstats`) must be **installed** (via packages) on
    the system.

.. note::
    PoWA needs to be **enabled** via the :ref:`site settings<settings>`.

Once created the instance can be `registered`_ on the PoWA repository.



.. _`Prometheus postgres_exporter`: https://github.com/prometheus-community/postgres_exporter
.. _`PoWA`: https://powa.readthedocs.io/en/latest/
.. _`Remote setup`: https://powa.readthedocs.io/en/latest/remote_setup.html
.. _`registered`: https://powa.readthedocs.io/en/latest/components/powa-archivist/configuration.html#powa-register-server
