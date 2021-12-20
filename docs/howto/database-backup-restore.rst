Databases backup and restore
----------------------------

Programs ``pg_dump`` and ``pg_restore`` can be used directly through ``pglift
instance exec`` command.

.. code-block:: console

    $ pglift instance exec 14/main -- pg_dump -Fc mydb -j4 -f mydb.dump
    $ pglift instance exec 14/main -- pg_restore -d postgres --clean --create mydb.dump
