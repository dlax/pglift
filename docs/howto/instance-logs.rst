Access instance logs
--------------------

When the PostgreSQL instance is configured to record logs to file(s), i.e.
with configuration parameter ``log_destination`` containing ``stderr`` and/or
``csvlog``, command ``pglift instance logs`` can be used to retrieve the
current log file:

.. code-block:: console

    $ pglift instance logs main
    2022-01-26 09:46:58.556 CET [29955] LOG:  starting PostgreSQL 14.1 (Debian 14.1-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
    2022-01-26 09:46:58.557 CET [29955] LOG:  listening on IPv6 address "::1", port 5999
    2022-01-26 09:46:58.557 CET [29955] LOG:  listening on IPv4 address "127.0.0.1", port 5999
    2022-01-26 09:46:58.559 CET [29955] LOG:  listening on Unix socket "/tmp/postgresql/.s.PGSQL.5999"
    2022-01-26 09:46:58.563 CET [29957] LOG:  database system was shut down at 2022-01-26 09:46:58 CET
    2022-01-26 09:46:58.568 CET [29955] LOG:  database system is ready to accept connections
    2022-01-26 09:49:15.928 CET [29955] LOG:  received SIGHUP, reloading configuration files
    2022-01-26 09:49:15.929 CET [29955] LOG:  parameter "log_connections" changed to "on"
    2022-01-26 09:49:15.929 CET [29955] LOG:  parameter "log_disconnections" changed to "on"
    2022-01-26 09:49:26.832 CET [30794] LOG:  connection received: host=[local]
    2022-01-26 09:49:26.832 CET [30794] LOG:  connection authorized: user=postgres database=postgres application_name=psql
    2022-01-26 09:51:12.860 CET [30794] LOG:  disconnection: session time: 0:01:46.028 user=postgres database=postgres host=[local]
    2022-01-26 10:17:34.089 CET [34327] LOG:  connection received: host=[local]
    2022-01-26 10:17:34.090 CET [34327] LOG:  connection authorized: user=postgres database=postgres application_name=psql
    2022-01-26 10:18:31.141 CET [34327] LOG:  disconnection: session time: 0:00:57.051 user=postgres database=postgres host=[local]
    2022-01-26 10:18:32.669 CET [34612] LOG:  connection received: host=[local]
    2022-01-26 10:18:32.670 CET [34612] LOG:  connection authorized: user=postgres database=postgres application_name=psql
    2022-01-26 10:18:37.277 CET [34612] LOG:  disconnection: session time: 0:00:04.607 user=postgres database=postgres host=[local]
    2022-01-26 10:32:41.444 CET [37973] LOG:  connection received: host=[local]
    2022-01-26 10:32:41.445 CET [37973] LOG:  connection authorized: user=postgres database=postgres application_name=psql
    2022-01-26 10:32:47.878 CET [37973] LOG:  disconnection: session time: 0:00:06.434 user=postgres database=postgres host=[local]
    2022-01-26 10:39:30.958 CET [39039] LOG:  connection received: host=[local]
    2022-01-26 10:39:30.958 CET [39039] LOG:  connection authorized: user=postgres database=postgres application_name=psql
    2022-01-26 10:40:28.514 CET [39039] LOG:  disconnection: session time: 0:00:57.556 user=postgres database=postgres host=[local]

Check how instance logging is configured as follows:

.. code-block:: console

    $ pglift instance config show main log_destination
    log_destination = 'stderr'
