Access instance shell (psql)
----------------------------

Command ``pglift instance exec`` can be used to enter a ``psql`` shell on
target instance or to execute any program in PostgreSQL binary directory.

.. code-block:: console

    $ pglift instance exec 14/main -- psql
    psql (14.1 (Debian 14.1-1.pgdg110+1))
    Type "help" for help.

    postgres@postgres=# \q
    $ pglift instance exec 14/main -- psql "dbname=test" -c '\x' -c 'SELECT * FROM foo;'
    Expanded display is on.
    -[ RECORD 1 ]
    x | 1
    -[ RECORD 2 ]
    x | 2

    $ pglift instance exec test -- pg_isready
    /home/dba/.local/share/pglift/run/postgresql:5432 - no response
