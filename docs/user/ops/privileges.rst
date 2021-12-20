Access privileges
=================

Command line interface
----------------------

The ``instance``, ``role`` and ``database`` command line entry points expose a
``privileges`` command that will list default access privileges.

At instance level, ``pglift instance privileges <instance name> [<version>]``
would list privileges for all roles and databases of the instance, unless a
``--role`` and/or a ``--database`` option is specified:

.. code-block:: console

    $ pglift instance privileges main
    database    schema    role    object_type    privileges
    ----------  --------  ------  -------------  -----------------------------------------------------------------------------
    myapp       public    manuel  TABLE          ['DELETE', 'INSERT', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE']
    otherapp    public    manuel  FUNCTION       ['EXECUTE']
    postgres    public    manuel  TABLE          ['DELETE', 'INSERT', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE']
    $ pglift instance privileges main --database=postgres --json
    [
      {
        "database": "postgres",
        "schema": "public",
        "role": "manuel",
        "object_type": "TABLE",
        "privileges": [
          "DELETE",
          "INSERT",
          "REFERENCES",
          "SELECT",
          "TRIGGER",
          "TRUNCATE",
          "UPDATE"
        ]
      }
    ]

At database (resp. role) level, ``pglift database privileges <version>/<name>
<dbname>`` (resp. ``pglift role privileges <version>/<name> <rolname>``) would
list privileges for specified database (resp. role):

.. code-block:: console

    $ pglift database privileges 13/main myapp
    database    schema    role    object_type    privileges
    ----------  --------  ------  -------------  -----------------------------------------------------------------------------
    myapp       public    manuel  TABLE          ['DELETE', 'INSERT', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE']
