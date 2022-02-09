PostgreSQL authentication
=========================

In site settings, the ``postgresql.auth`` allow to configure PostgreSQL
authentication. Default values are:

.. code-block:: json

    "auth": {
      "local": "trust",
      "host": "trust",
      "passfile": "$HOME/.pgpass",
      "password_command": null
    },

For a production cluster it's recommended to set ``local`` and ``host``
authentication to one of the supported `authentication methods`_.

Many `pglift` operations require a database access to the target instance using
the super-user role (``postgres`` by default). Unless the authentication policy
is set to ``trust`` a password would then be required for each operations.

In site settings, the ``postgresql.surole.pgpass`` configuration option, when
set to ``true``, will write a `password file`_ (``pgpass``) entry for the
super-user role.

At instance creation, one can define a password for the super-user role
(``postgres`` by default), using ``--surole-password`` option to ``pglift
instance create`` or via a yaml manifest:

.. code-block:: yaml
    :caption: my-instance.yaml

    name: main
    surole_password: s3kre!t

When the password file is used, nothing special is required for authentication
as all libpq operations would use it.

Otherwise, the password is read from ``PGPASSWORD`` environment variable so
this should be set in the environment running interactive commands.

Alternatively, you can configure a ``postgresql.auth.password_command`` option
in site settings, it can be any user-managed executable command and `pglift`
and must return the super-user role password as stdout.

.. _`password file`: https://www.postgresql.org/docs/current/libpq-pgpass.html
.. _`authentication methods`: https://www.postgresql.org/docs/current/auth-methods.html
