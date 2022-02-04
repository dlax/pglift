Databases maintenance operations
--------------------------------

Maintenance operations on databases, such as ``ANALYZE`` or ``VACUUM`` can be
run through the ``pglift database run`` command.

Examples:

.. code-block:: console

    $ pglift database -i 13/main run -d myapp "ANALYZE VERBOSE"

would run ``ANALYZE VERBOSE`` on database "myapp" of instance `13/main`.

.. code-block:: console

    $ pglift database -i 13/main run -x test "VACUUM FULL VERBOSE"

would run ``VACUUM FULL VERBOSE`` on all databases of instance `13/main`
except the one named "test".

This is also accessible from :func:`pglift.databases.run`.
