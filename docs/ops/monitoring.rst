Monitoring
==========

Instance monitoring is handled by `Prometheus postgres_exporter`_ for which is
service is deployed at instance creation.

Command line interface
----------------------

The ``postgres_exporter`` command line entry point exposes the following
``start`` and ``stop`` commands to handle postgres_exporter service.

Ansible module
--------------

The ``postgres_exporter`` module within ``dalibo.pglift`` collection is the
main entry point for managing a `postgres_exporter` service for a non-local
instance through Ansible.

Example task:

.. code-block:: yaml

    tasks:
      - dalibo.pglift.postgres_exporter:
          name: 13-main  # usually a reference to target instance
          dsn: "port=5455 host=dbserver.example.com role=monitoring password=m0n1tor"
          port: 9871

Python API
----------

.. currentmodule:: pglift.prometheus

Module :mod:`pglift.prometheus` exposes the following API functions for
monitoring management using `Prometheus postgres_exporter`_:

.. autofunction:: setup
.. autofunction:: drop
.. autofunction:: apply
.. autofunction:: exists
.. autofunction:: port
.. autofunction:: start
.. autofunction:: stop

.. _`Prometheus postgres_exporter`: https://github.com/prometheus-community/postgres_exporter
