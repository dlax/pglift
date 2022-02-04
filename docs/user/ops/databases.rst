Databases operations
====================

Command line interface
----------------------

The ``pglift database`` command line entry point exposes commands to
manage PostgreSQL databases of an instance.

.. code-block:: console

    $ pglift database --help
    Usage: pglift database [OPTIONS] COMMAND [ARGS]...

      Manage databases.

    Options:
      -i, --instance <version>/<name>
                                      Instance identifier; the <version>/ prefix
                                      may be omitted if there's only one instance
                                      matching <name>. Required if there is more
                                      than one instance on system.
      --schema                        Print the JSON schema of database model and
                                      exit.
      --help                          Show this message and exit.

    Commands:
      alter       Alter a database in a PostgreSQL instance
      apply       Apply manifest as a database
      create      Create a database in a PostgreSQL instance
      describe    Describe a database
      drop        Drop a database
      list        List databases
      privileges  List default privileges on a database.
      run         Run given command on databases of a PostgreSQL instance


Ansible module
--------------

The ``database`` module within ``dalibo.pglift`` collection is the main entry
point for PostgreSQL databases management through Ansible.

Example task:

.. code-block:: yaml

    tasks:
      - name: my database
        dalibo.pglift.database:
          instance: myinstance
          name: myapp
          owner: dba
