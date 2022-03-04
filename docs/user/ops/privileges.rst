Default access privileges
=========================

Command line interface
----------------------

The ``instance``, ``role`` and ``database`` command line entry points expose a
``privileges`` command that will list default access privileges.

In psql, ``\ddp`` command can be used to access existing assignments of default privileges.

At instance level, ``pglift instance privileges <instance name> [<version>]``
would list default privileges for all roles and databases of the instance, unless a
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
list default privileges for specified database (resp. role):

.. code-block:: console

    $ pglift database -i 13/main privileges myapp
    database    schema    role    object_type    privileges
    ----------  --------  ------  -------------  -----------------------------------------------------------------------------
    myapp       public    manuel  TABLE          ['DELETE', 'INSERT', 'REFERENCES', 'SELECT', 'TRIGGER', 'TRUNCATE', 'UPDATE']

Alter default parameters
^^^^^^^^^^^^^^^^^^^^^^^^

*PostgreSQL grants privileges on some types of objects to PUBLIC by default when
the objects are created. No privileges are granted to PUBLIC by default on tables,
table columns, sequences, foreign data wrappers, foreign servers, large objects,
schemas, or tablespaces.* [#f1]_

To override `default privileges`_ settings, use the ALTER DEFAULT PRIVILEGES command.

.. code-block:: console

    $ pglift database -i 13/main run -d myapp "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO dba"
    INFO     running "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO dba" on myapp database of 13/main
    INFO     ALTER DEFAULT PRIVILEGES

Different commands can be used to view the results :

.. code-block:: console

    $ pglift instance exec 13/main -- psql -d myapp -c '\ddp'
            Droits d'accès par défaut
     Propriétaire | Schéma | Type  |    Droits d'accès
    --------------+--------+-------+----------------------
     postgres     | public | table | dba=arwdDxt/postgres
    (1 ligne)

or

.. code-block:: console

    $ pglift database -i 13/main privileges myapp
     ───────────────────────────────────────────────────────────────────────────────────────────
    | database | schema | role | object_type | privileges                                       |
    └──────────┴────────┴──────┴─────────────┴──────────────────────────────────────────────────┘
    │ myapp    │ public │ dba  │ TABLE       │ ['DELETE', 'INSERT', 'REFERENCES', 'SELECT',     │
    │          │        │      │             │ 'TRIGGER', 'TRUNCATE', 'UPDATE']                 │
    └──────────┴────────┴──────┴─────────────┴──────────────────────────────────────────────────┘

.. [#f1]
   See the `privileges documentation`_.

.. _`privileges documentation`: https://www.postgresql.org/docs/current/ddl-priv.html
.. _`default privileges`: https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html
