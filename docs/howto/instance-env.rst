Instance (libpq) environment
----------------------------

Command ``pglift instance env`` gives access to the libpq environment for
target instance.

.. code-block:: console

    $ pglift instance env 14/main
    PATH=/usr/lib/postgresql/14/bin:/usr/local/bin:/usr/bin:/bin:/usr/local/games:/usr/games
    PGDATA=/home/dba/.local/share/pglift/srv/pgsql/14/main/data
    PGHOST=/home/dba/.local/share/pglift/run/postgresql
    PGPASSFILE=/home/dba/.pgpass
    PGPORT=5456
    PGUSER=postgres

This can be used in shell scripts. For instance:

.. code-block:: sh

    #!/bin/sh
    export $(pglift instance env main)
    psql -d db -c 'select 1'
    pg_dump -d db > db.sql
