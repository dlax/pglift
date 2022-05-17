pgBackRest site configuration
=============================

pgBackRest satellite component can be enabled through site settings by
defining a non-``null`` ``pgbackrest`` key, e.g.:

.. code-block:: yaml
   :caption: settings.yaml

    pgbackrest:
      execpath: /usr/local/bin/pgbackrest
      directory: /backups/{instance.version}-{instance.name}

.. note::
   Use ``pglift site-settings --schema`` (possibly piped through ``jq
   .definitions.PgBackRestSettings``) to retrieve possible settings for the
   ``pgbackrest`` section.

Upon instance creation, pgBackRest is set up by:

* adding some configuration to PostgreSQL,
* writing a pgBackRest configuration file for this instance, and,
* initializing the pgBackRest repository.

PostgreSQL configuration is handled through a ``pgbackrest.conf`` file
installed in ``conf.pglift.d`` as documented in :ref:`PostgreSQL site
configuration <postgresql-site-configuration>`. The default template for this
file is:

.. literalinclude:: ../../../src/pglift/data/postgresql/pgbackrest.conf
   :caption: $PGDATA/conf.pglift.d/pgbackrest.conf

and it might be overridden by providing a different template file in site
configuration directory.
