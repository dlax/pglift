Standby setup
-------------

Creating a standby instance:

::

    $ pglift instance create standby --standby-for <primary dsn>


The ``--standby-for`` option should be a `connection string`_ to the primary
server (e.g. ``host=primary port=5433``).
If the primary is also a pglift instance, you must use the dedicated
``replication`` user, set ``user=replication`` in the dsn.

pglift will call `pg_basebackup`_ utility to create a standby. A replication
slot can be specified with ``--standby-slot <slot name>``.


Promoting a standby instance:

::

    $ pglift instance promote standby

.. _`connection string`: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
.. _pg_basebackup: https://www.postgresql.org/docs/current/app-pgbasebackup.html
