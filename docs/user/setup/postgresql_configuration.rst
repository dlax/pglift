.. _`postgresql-site-configuration`:

PostgreSQL configuration
========================

Instances created by pglift have their configuration managed. This is handled
by writing configuration items into ``postgresql.conf`` within instance's data
directory.

Instance configuration is built from
site-wise ``postgresql.conf`` file (or a template) that might be present in
``$XDG_CONFIG_HOME/pglift/postgresql/postgresql.conf`` [#xdgconfighome]_ or
``/etc/pglift/postgresql/postgresql.conf`` (by order of precedence). The
default ``postgresql.conf`` template file contains:

.. literalinclude:: ../../../src/pglift/data/postgresql/postgresql.conf
   :language: ini
   :caption: postgresql.conf

Memory settings (``shared_buffers`` and ``effective_cache_size``) may be
specified as a percentage of total system memory. ``unix_socket_directories``
value will be computed from site settings for the ``socket_directory`` defined
in ``postgresql`` section.

Then, some satellite components (e.g. pgBackRest) may define some PostgreSQL
configuration settings they might need for proper operation.

Finally, any user-supplied value (e.g. the ``port`` or ``locale`` options or
any other one provided through, e.g., the Ansible module) overrides the
configuration:

::

    $ pglift instance create --port=5678 --locale=fr_FR --encoding=latin1 main
    $ pglift instance exec main -- psql -c "select name, setting from pg_settings where source = 'configuration file';"
              name           |                                                         setting
    -------------------------+--------------------------------------------------------------------------------------------------------------------------
     archive_command         | /usr/bin/pgbackrest --config=/etc/pgbackrest/pgbackrest-14-main.conf --stanza=14-main archive-push %p
     archive_mode            | on
     cluster_name            | main
     effective_cache_size    | 524288
     lc_messages             | fr_FR
     lc_monetary             | fr_FR
     lc_numeric              | fr_FR
     lc_time                 | fr_FR
     log_destination         | stderr
     logging_collector       | on
     port                    | 5678
     shared_buffers          | 131072
     unix_socket_directories | /run/user/1000/pglift/postgresql
     wal_level               | replica
    (14 rows)



.. seealso::
   The ``pgconf`` command to manage :ref:`PostgreSQL configuration <pgconf>`.
   ::

        $ pglift pgconf -i main show
        port = 5678
        unix_socket_directories = '/run/user/1000/pglift/postgresql'
        shared_buffers = '1 GB'
        wal_level = 'replica'
        archive_mode = on
        archive_command = '/usr/bin/pgbackrest --config=/etc/pgbackrest/pgbackrest-14-main.conf --stanza=14-main archive-push %p'
        effective_cache_size = '4 GB'
        log_destination = 'stderr'
        logging_collector = on
        cluster_name = 'main'
        lc_messages = 'fr_FR'
        lc_monetary = 'fr_FR'
        lc_numeric = 'fr_FR'
        lc_time = 'fr_FR'

.. [#xdgconfighome]
   Where ``$XDG_CONFIG_DIR`` would be ``$HOME/.config`` unless configured
   differently.
