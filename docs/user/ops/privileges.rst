Access privileges
=================

The ``instance``, ``role`` and ``database`` command line entry points expose a
``privileges`` command that will list access privileges.

In psql, ``\dp`` and ``\ddp`` commands can be used to access existing assignments
of privileges and default privileges respectively.

Listing privileges
------------------

At instance level, ``pglift instance privileges <instance name> [<version>]``
would list privileges for all roles and databases of the instance, unless a
``--role`` and/or a ``--database`` option is specified:

.. code-block:: console

    $ pglift instance privileges 13/main
                                                                Privileges on instance 13/main
    ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   │ database   │ schema │ object_type │ role     │ privileges                                                    │ object_name │ column_privileges      │
   └────────────┴────────┴─────────────┴──────────┴───────────────────────────────────────────────────────────────┴─────────────┴────────────────────────┘
   │ myapp      │ public │ TABLE       │ dba      │                                                               │ persons     │ name: SELECT, UPDATE   │
   │ myapp      │ public │ TABLE       │ postgres │ INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER │ persons     │                        │
   │ myotherapp │ public │ TABLE       │ dba      │ TRUNCATE, INSERT, SELECT, UPDATE, DELETE, REFERENCES, TRIGGER │ city        │                        │
   │ myotherapp │ public │ TABLE       │ marion   │                                                               │ city        │ name: SELECT           │
   │            │        │             │          │                                                               │             │ postcode: UPDATE       │
   │ myotherapp │ public │ TABLE       │ postgres │ REFERENCES, TRUNCATE, DELETE, UPDATE, SELECT, INSERT, TRIGGER │ city        │                        │
   │ myotherapp │ public │ TABLE       │ bob      │                                                               │ garden      │ species: SELECT        │
   │ myotherapp │ public │ TABLE       │ dba      │ UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, INSERT, SELECT │ garden      │ flower: SELECT, UPDATE │
   │ myotherapp │ public │ TABLE       │ marion   │ UPDATE                                                        │ garden      │ species: UPDATE        │
   │            │        │             │          │                                                               │             │ flower: SELECT         │
   │ myotherapp │ public │ TABLE       │ postgres │ TRUNCATE, DELETE, UPDATE, SELECT, REFERENCES, INSERT, TRIGGER │ garden      │                        │
   └────────────┴────────┴─────────────┴──────────┴───────────────────────────────────────────────────────────────┴─────────────┴────────────────────────┘


    $ pglift instance privileges 13/main --database=myapp --json
    [
      {
        "database": "myapp",
        "schema": "public",
        "object_type": "TABLE",
        "role": "dba",
        "privileges": null,
        "object_name": "persons",
        "column_privileges": {
          "name": [
            "SELECT",
            "UPDATE"
          ]
        }
      },
      {
        "database": "myapp",
        "schema": "public",
        "object_type": "TABLE",
        "role": "postgres",
        "privileges": [
          "INSERT",
          "SELECT",
          "UPDATE",
          "DELETE",
          "TRUNCATE",
          "REFERENCES",
          "TRIGGER"
        ],
        "object_name": "persons",
        "column_privileges": null
      }
    ]


You can also access the privileges for a `database` or a `role` by using the
dedicated commands.

For example, the following commands are equivalent:

.. code-block:: console

    $ pglift instance privileges 13/main -d myotherapp
    $ pglift database -i 13/main privileges myotherapp


Alter privileges
----------------

To assign `privileges`_ settings to an object, sql query can be passed through
the run command:

  .. code-block:: console

      $ pglift database -i 13/main run -d myapp "GRANT UPDATE ON mytable TO dba"
      INFO     running GRANT UPDATE ON mytable TO dba" on myapp database of 13/main
      INFO     GRANT

**Default privileges**

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

    $ pglift database -i 13/main privileges myapp --default
     ────────────────────────────────────────────────────────────────────────────────────────
    | database | schema | role | object_type | privileges                                    |
    └──────────┴────────┴──────┴─────────────┴───────────────────────────────────────────────┘
    │ myapp    │ public │ dba  │ TABLE       │ DELETE, INSERT, REFERENCES, SELECT, TRIGGER,  │
    │          │        │      │             │ TRUNCATE, UPDATE                              │
    └──────────┴────────┴──────┴─────────────┴───────────────────────────────────────────────┘

.. [#f1]
   See the `privileges documentation`_.

.. _`privileges`: https://www.postgresql.org/docs/current/ddl-priv.html
.. _`privileges documentation`: https://www.postgresql.org/docs/current/ddl-priv.html
.. _`default privileges`: https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html
