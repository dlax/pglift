import pathlib
from typing import Any, Dict, List, Sequence, Tuple

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from . import db, exceptions, types
from .ctx import BaseContext
from .models import interface
from .models.system import Instance
from .task import task


def apply(
    ctx: BaseContext, instance: Instance, database_manifest: interface.Database
) -> None:
    """Apply state described by specified database manifest as a PostgreSQL instance.

    The instance should be running.
    """
    if database_manifest.state == interface.Database.State.absent:
        if exists(ctx, instance, database_manifest.name):
            drop(ctx, instance, database_manifest.name)
        return None

    if not exists(ctx, instance, database_manifest.name):
        create(ctx, instance, database_manifest)
    else:
        alter(ctx, instance, database_manifest)


def describe(ctx: BaseContext, instance: Instance, name: str) -> interface.Database:
    """Return a database described as a manifest.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.superuser_connect(instance) as cnx:
        with cnx.cursor() as cur:
            cur.execute(db.query("database_inspect"), {"datname": name})
            values = dict(cur.fetchone())
    return interface.Database(name=name, **values)


def list(ctx: BaseContext, instance: Instance) -> List[interface.DetailedDatabase]:
    """List all databases in instance."""

    with db.superuser_connect(instance) as cnx:
        psycopg2.extensions.register_type(
            # select typarray from pg_type where typname = 'aclitem'; -> 1034
            psycopg2.extensions.new_array_type((1034,), "ACLITEM[]", psycopg2.STRING)
        )
        with cnx.cursor() as cur:
            cur.execute(db.query("database_list"))
            values = cur.fetchall()
    return [interface.DetailedDatabase(**v) for v in values]


@task("drop '{name}' database from instance {instance}")
def drop(ctx: BaseContext, instance: Instance, name: str) -> None:
    """Drop a database from instance.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.superuser_connect(instance, autocommit=True) as cnx:
        with cnx.cursor() as cur:
            cur.execute(db.query("database_drop", database=sql.Identifier(name)))


def exists(ctx: BaseContext, instance: Instance, name: str) -> bool:
    """Return True if named database exists in 'instance'.

    The instance should be running.
    """
    with db.superuser_connect(instance) as cnx:
        with cnx.cursor() as cur:
            cur.execute(db.query("database_exists"), {"database": name})
            return cur.rowcount == 1  # type: ignore[no-any-return]


def options_and_args(
    database: interface.Database,
) -> Tuple[sql.Composable, Dict[str, Any]]:
    """Return the "options" part of CREATE DATABASE or ALTER DATABASE SQL
    commands based on 'database' model along with query arguments.
    """
    opts = []
    args: Dict[str, Any] = {}
    if database.owner is not None:
        opts.append(
            sql.SQL(" ").join([sql.SQL("OWNER"), sql.Identifier(database.owner)])
        )
    return sql.SQL(" ").join(opts), args


@task("create '{database.name}' database on instance {instance}")
def create(ctx: BaseContext, instance: Instance, database: interface.Database) -> None:
    """Create 'database' in 'instance'.

    The instance should be running and the database should not exist already.
    """
    options, args = options_and_args(database)
    with db.superuser_connect(instance, autocommit=True) as cnx:
        with cnx.cursor() as cur:
            cur.execute(
                db.query(
                    "database_create",
                    database=sql.Identifier(database.name),
                    options=options,
                ),
                args,
            )


@task("alter '{database.name}' database on instance {instance}")
def alter(ctx: BaseContext, instance: Instance, database: interface.Database) -> None:
    """Alter 'database' in 'instance'.

    The instance should be running and the database should exist already.
    """
    if not exists(ctx, instance, database.name):
        raise exceptions.DatabaseNotFound(database.name)

    if database.owner is None:
        owner = sql.SQL("CURRENT_USER")
    else:
        owner = sql.Identifier(database.owner)
    options = sql.SQL(" ").join([sql.SQL("OWNER TO"), owner])
    with db.superuser_connect(instance) as cnx:
        with cnx.cursor() as cur:
            cur.execute(
                db.query(
                    "database_alter_owner",
                    database=sql.Identifier(database.name),
                    options=options,
                ),
            )
        cnx.commit()


@task("backup '{name}' database on instance {instance}")
def backup(
    ctx: BaseContext, instance: Instance, name: str, output_file: pathlib.Path
) -> None:
    """Dump a database.

    The instance should be running and the database should exist already.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)

    bindir = ctx.pg_ctl(instance.version).bindir
    config = instance.config()
    try:
        host = config.unix_socket_directories.split(",")[0]  # type: ignore[union-attr]
    except (AttributeError, IndexError):
        host = "localhost"
    user = ctx.settings.postgresql.surole.name
    cmd = [
        str(bindir / "pg_dump"),
        "--port",
        str(instance.port),
        "--host",
        host,
        "--user",
        user,
        "-Fc",
        "-f",
        str(output_file),
        name,
    ]

    env = ctx.settings.postgresql.auth.libpq_environ()
    ctx.run(cmd, check=True, env=env)


def run(
    ctx: BaseContext,
    instance: Instance,
    sql_command: str,
    *,
    dbnames: Sequence[str] = (),
    exclude_dbnames: Sequence[str] = (),
    notice_handler: types.NoticeHandler = db.default_notice_handler,
) -> None:
    for database in list(ctx, instance):
        if (
            dbnames and database.name not in dbnames
        ) or database.name in exclude_dbnames:
            continue
        with db.superuser_connect(
            instance, dbname=database.name, autocommit=True
        ) as cnx:
            cnx.notices = notice_handler
            with cnx.cursor() as cur:
                ctx.info("run %s on database %s of %s", sql_command, database, instance)
                cur.execute(sql_command)
