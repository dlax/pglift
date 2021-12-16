from typing import Any, Dict, List, Sequence, Tuple

import psycopg.rows
from psycopg import sql

from . import db, exceptions, logger, types
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
    with db.superuser_connect(ctx, instance) as cnx:
        with cnx.cursor(row_factory=psycopg.rows.class_row(interface.Database)) as cur:
            row = cur.execute(
                db.query("database_inspect"), {"datname": name}
            ).fetchone()
        assert row is not None
        return row


def list(ctx: BaseContext, instance: Instance) -> List[interface.DetailedDatabase]:
    """List all databases in instance."""

    with db.superuser_connect(ctx, instance) as cnx:
        with cnx.cursor(
            row_factory=psycopg.rows.class_row(interface.DetailedDatabase)
        ) as cur:
            cur.execute(db.query("database_list"))
            return cur.fetchall()


@task("drop '{name}' database from instance {instance}")
def drop(ctx: BaseContext, instance: Instance, name: str) -> None:
    """Drop a database from instance.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.superuser_connect(ctx, instance, autocommit=True) as cnx:
        cnx.execute(db.query("database_drop", database=sql.Identifier(name)))


def exists(ctx: BaseContext, instance: Instance, name: str) -> bool:
    """Return True if named database exists in 'instance'.

    The instance should be running.
    """
    with db.superuser_connect(ctx, instance) as cnx:
        cur = cnx.execute(db.query("database_exists"), {"database": name})
        return cur.rowcount == 1


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
    with db.superuser_connect(ctx, instance, autocommit=True) as cnx:
        cnx.execute(
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

    owner: sql.Composable
    if database.owner is None:
        owner = sql.SQL("CURRENT_USER")
    else:
        owner = sql.Identifier(database.owner)
    options = sql.SQL(" ").join([sql.SQL("OWNER TO"), owner])
    with db.superuser_connect(ctx, instance) as cnx:
        cnx.execute(
            db.query(
                "database_alter_owner",
                database=sql.Identifier(database.name),
                options=options,
            ),
        )
        cnx.commit()


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
            ctx, instance, dbname=database.name, autocommit=True
        ) as cnx:
            cnx.add_notice_handler(notice_handler)
            logger.info("run %s on database %s of %s", sql_command, database, instance)
            cnx.execute(sql_command)
