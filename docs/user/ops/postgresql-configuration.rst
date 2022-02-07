.. _pgconf:

PostgreSQL configuration
========================

The ``pglift pgconf`` command line entry point exposes commands to manage
configuration of a PostgreSQL instance.

.. code-block:: console

    $ pglift pgconf --help
    Usage: pglift pgconf [OPTIONS] COMMAND [ARGS]...

      Manage configuration of a PostgreSQL instance.

    Options:
      --help  Show this message and exit.

    Commands:
      edit    Edit managed configuration.
      remove  Remove configuration items.
      set     Set configuration items.
      show    Show configuration (all parameters or specified ones).

It operates only on configuration files and does not assume that the instance
is started. To make changes effective, the user may need to restart or reload
the instance, see :doc:`/user/ops/instance`.

.. warning:: Some configuration settings should not be modified through this
   command as they may be needed for other satellite services to work.
   Typically, the ``port`` setting is one of them. Similarly, the selected
   backup system may assume that some parameter are set to particular values.

Show the configuration
^^^^^^^^^^^^^^^^^^^^^^

View specific parameter:

.. code-block:: console

    $ pglift pgconf show main log_connections
    log_connections = off

View multiple parameters:

.. code-block:: console

    $ pglift pgconf show main log_connections log_disconnections
    log_connections = off
    log_disconnections = off

View all parameters:

.. code-block:: console

    $ pglift pgconf show main
    archive_command = '/usr/bin/pgbackrest --config=/etc/pgbackrest/pgbackrest-14-main.conf --stanza=14-main archive-push %p'
    archive_mode = on
    max_wal_senders = 3
    wal_level = 'replica'
    cluster_name = 'main'
    shared_buffers = '128MB'
    effective_cache_size = '5 GB'
    unix_socket_directories = '/var/run/postgresql'
    log_destination = 'stderr'
    logging_collector = on
    port = 5454
    max_connections = 100
    dynamic_shared_memory_type = 'posix'
    max_wal_size = '1GB'
    min_wal_size = '80MB'
    log_timezone = 'Europe/Paris'
    datestyle = 'iso, mdy'
    timezone = 'Europe/Paris'
    lc_messages = 'C'
    lc_monetary = 'C'
    lc_numeric = 'C'
    lc_time = 'C'
    default_text_search_config = 'pg_catalog.english'

Change the configuration
^^^^^^^^^^^^^^^^^^^^^^^^

Set one parameter:

.. code-block:: console

    $ pglift pgconf set main log_connections=on
    log_connections: off -> on

Set multiple parameters:

.. code-block:: console

    $ pglift pgconf set main log_connections=on log_disconnections=on
    log_connections: off -> on
    log_disconnections: off -> on

.. note::
    To directly edit the configuration file, use:

    .. code-block:: console

        $ pglift pgconf edit main

    this will open your text editor with the *user* configuration.

Remove parameters configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Remove specific parameter:

.. code-block:: console

  $ pglift pgconf remove main log_connections
  log_connections: on -> None

Remove multiple parameters:

.. code-block:: console

  $ pglift pgconf remove main log_connections log_disconnections
  log_connections: on -> None
  log_disconnections: on -> None
