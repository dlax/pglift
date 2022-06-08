Standby setup
-------------

Creating a standby instance:

::

    $ pglift instance create standby --standby-for <primary dsn> --standby-password
    Password for the replication user:
    Repeat for confirmation:


The ``--standby-for`` option should be a `connection string`_ to the primary
server (e.g. ``host=primary port=5433``).
If the primary is also a pglift instance, you must use the dedicated
``replication`` user, set ``user=replication`` in the dsn.

pglift will call `pg_basebackup`_ utility to create a standby. A replication
slot can be specified with ``--standby-slot <slot name>``.

.. note::
   If the primary instance has a password set for the super-user role, and is
   needed for local authentication through the password file in particular, it
   might be useful to provide the same password through ``--surole-password``
   option when creating the standby.

.. note::
   If Prometheus postgres_exporter was set up on the primary instance and is
   wanted on the standby, don't forget to provide ``--prometheus-password``
   option to the above command with the same password as on the primary
   instance.

Promoting a standby instance:

::

    $ pglift instance promote standby

.. _`connection string`: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
.. _pg_basebackup: https://www.postgresql.org/docs/current/app-pgbasebackup.html
