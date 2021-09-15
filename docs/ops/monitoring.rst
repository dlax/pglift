Monitoring
==========

Instance monitoring is handled by `Prometheus postgres_exporter`_ for which is
service is deployed at instance creation.

Command line interface
----------------------

The ``postgres_exporter`` command line entry point exposes the following
``start`` and ``stop`` commands to handle postgres_exporter service.

Python API
----------

.. currentmodule:: pglift.prometheus

Module :mod:`pglift.prometheus` exposes the following API functions for
monitoring management using `Prometheus postgres_exporter`_:

.. autofunction:: setup
.. autofunction:: port
.. autofunction:: start
.. autofunction:: stop

.. _`Prometheus postgres_exporter`: https://github.com/prometheus-community/postgres_exporter
