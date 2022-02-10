import logging
from typing import TYPE_CHECKING, Any, Dict, List, Sequence, Tuple

import psycopg.rows
from pgtoolkit import conf as pgconf
from psycopg import sql

from . import db, exceptions, types
from .ctx import BaseContext
from .models import interface
from .task import task

if TYPE_CHECKING:
    from .models import system

logger = logging.getLogger(__name__)


def apply(
    ctx: BaseContext, instance: "system.Instance", database_manifest: interface.Database
) -> None:
    """Apply state described by specified database manifest as a PostgreSQL instance.

    The instance should be running.
    """
    if database_manifest.state == interface.PresenceState.absent:
        if exists(ctx, instance, database_manifest.name):
            drop(ctx, instance, database_manifest.name)
        return None

    if not exists(ctx, instance, database_manifest.name):
        create(ctx, instance, database_manifest)
    else:
        alter(ctx, instance, database_manifest)


def describe(
    ctx: BaseContext, instance: "system.Instance", name: str
) -> interface.Database:
    """Return a database described as a manifest.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.superuser_connect(ctx, instance) as cnx:
        row = cnx.execute(db.query("database_inspect"), {"datname": name}).fetchone()
        assert row is not None
        settings = row.pop("settings")
        if settings is None:
            row["settings"] = None
        else:
            row["settings"] = {}
            for s in settings:
                k, v = s.split("=", 1)
                row["settings"][k.strip()] = pgconf.parse_value(v.strip())
        return interface.Database.parse_obj(row)


def list(
    ctx: BaseContext, instance: "system.Instance"
) -> List[interface.DetailedDatabase]:
    """List all databases in instance."""

    with db.superuser_connect(ctx, instance) as cnx:
        with cnx.cursor(
            row_factory=psycopg.rows.class_row(interface.DetailedDatabase)
        ) as cur:
            cur.execute(db.query("database_list"))
            return cur.fetchall()


@task("dropping '{name}' database from instance {instance}")
def drop(ctx: BaseContext, instance: "system.Instance", name: str) -> None:
    """Drop a database from a primary instance.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.superuser_connect(ctx, instance, autocommit=True) as cnx:
        cnx.execute(db.query("database_drop", database=sql.Identifier(name)))


def exists(ctx: BaseContext, instance: "system.Instance", name: str) -> bool:
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
        opts.append(sql.SQL("OWNER {}").format(sql.Identifier(database.owner)))
    return sql.SQL(" ").join(opts), args


@task("creating '{database.name}' database on instance {instance}")
def create(
    ctx: BaseContext, instance: "system.Instance", database: interface.Database
) -> None:
    """Create 'database' in 'instance'.

    The instance should be a running primary and the database should not exist already.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
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
        if database.settings is not None:
            alter(ctx, instance, database)


@task("altering '{database.name}' database on instance {instance}")
def alter(
    ctx: BaseContext, instance: "system.Instance", database: interface.Database
) -> None:
    """Alter 'database' in 'instance'.

    The instance should be a running primary and the database should exist already.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)

    if not exists(ctx, instance, database.name):
        raise exceptions.DatabaseNotFound(database.name)

    owner: sql.Composable
    if database.owner is None:
        owner = sql.SQL("CURRENT_USER")
    else:
        owner = sql.Identifier(database.owner)
    options = sql.SQL("OWNER TO {}").format(owner)
    with db.superuser_connect(ctx, instance) as cnx:
        cnx.execute(
            db.query(
                "database_alter",
                database=sql.Identifier(database.name),
                options=options,
            ),
        )
        cnx.commit()

    if database.settings is not None:
        with db.superuser_connect(ctx, instance) as cnx:
            if not database.settings:
                # Empty input means reset all.
                cnx.execute(
                    db.query(
                        "database_alter",
                        database=sql.Identifier(database.name),
                        options=sql.SQL("RESET ALL"),
                    )
                )
            else:
                for k, v in database.settings.items():
                    if v is None:
                        options = sql.SQL("RESET {}").format(sql.Identifier(k))
                    else:
                        options = sql.SQL("SET {} TO {}").format(
                            sql.Identifier(k), sql.Literal(v)
                        )
                    cnx.execute(
                        db.query(
                            "database_alter",
                            database=sql.Identifier(database.name),
                            options=options,
                        )
                    )
            cnx.commit()


def run(
    ctx: BaseContext,
    instance: "system.Instance",
    sql_command: str,
    *,
    dbnames: Sequence[str] = (),
    exclude_dbnames: Sequence[str] = (),
    notice_handler: types.NoticeHandler = db.default_notice_handler,
) -> None:
    """Execute a SQL command on databases of `instance`.

    :param dbnames: restrict operation on databases with a name in this list.
    :param exclude_dbnames: exclude databases with a name in this list from
        the operation.
    :param notice_handler: a function to handle notice.
    """
    for database in list(ctx, instance):
        if (
            dbnames and database.name not in dbnames
        ) or database.name in exclude_dbnames:
            continue
        with db.superuser_connect(
            ctx, instance, dbname=database.name, autocommit=True
        ) as cnx:
            cnx.add_notice_handler(notice_handler)
            logger.info(
                'running "%s" on %s database of %s',
                sql_command,
                database.name,
                instance,
            )
            cnx.execute(sql_command)
