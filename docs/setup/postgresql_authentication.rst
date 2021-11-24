PostgreSQL authentication
=========================

Many operations require a database access to the target instance using the
super-user role (``postgres`` by default). Unless the authentication
policy is set to ``trust`` a password would then be required for each
operations.

In site settings, the ``postgresql.surole.pgpass`` configuration option, when
set to ``true``, will write a `password file`_ (``pgpass``) entry for the
super-user role.

At instance creation, one can define a password for the super-user role
(``postgres`` by default):

.. code-block:: yaml
    :caption: my-instance.yaml

    name: main
    surole_password: s3kre!t

When the password file is used, nothing special is required for authentication
as all libpq operations would use it.

Otherwise, the password is read from ``PGPASSWORD`` environment variable so
this should be set in the environment running interactive commands.

.. _`password file`: https://www.postgresql.org/docs/current/libpq-pgpass.html
