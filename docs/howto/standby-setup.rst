Standby setup
-------------

Creating a standby instance:

::

    $ pglift instance create standby --standby-for <primary dsn>


If the primary is also a pglift instance, you must use the dedicated
``replication`` user, set ``user=replication`` in the dsn.

pglift will call `pg_basebackup`_ utility to create a standby. A replication
slot can be specified with ``--standby-slot <slot name>``.


Promoting a standby instance:

::

    $ pglift instance promote standby

.. _pg_basebackup: https://www.postgresql.org/docs/current/app-pgbasebackup.html
