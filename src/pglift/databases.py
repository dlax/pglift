import datetime
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg.rows
from pgtoolkit import conf as pgconf
from psycopg import sql

from . import db, exceptions, hookimpl, types
from .ctx import BaseContext
from .models import interface, system
from .task import task

logger = logging.getLogger(__name__)


def apply(
    ctx: BaseContext,
    instance: "system.PostgreSQLInstance",
    database: interface.Database,
) -> Optional[bool]:
    """Apply state described by specified interface model as a PostgreSQL database.

    Return True, if changes were applied, False if no change is needed, and
    None if the database got dropped.

    The instance should be running.
    """
    name = database.name
    if database.state == interface.PresenceState.absent:
        if exists(ctx, instance, name):
            drop(ctx, instance, name)
            return None
        return False

    if not exists(ctx, instance, name):
        create(ctx, instance, database)
        return True

    actual = get(ctx, instance, name)
    alter(ctx, instance, database)
    return get(ctx, instance, name) != actual


def get(
    ctx: BaseContext, instance: "system.PostgreSQLInstance", name: str
) -> interface.Database:
    """Return the database object with specified name.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.connect(ctx, instance) as cnx:
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
        row["extensions"] = db.installed_extensions(ctx, instance, dbname=name)
        return interface.Database.parse_obj(row)


def list(
    ctx: BaseContext, instance: "system.PostgreSQLInstance", dbnames: Sequence[str] = ()
) -> List[interface.DetailedDatabase]:
    """List databases in instance.

    :param dbnames: restrict operation on databases with a name in this list.
    """
    where_clause: sql.Composable
    where_clause = sql.SQL("")
    if dbnames:
        where_clause = sql.SQL("AND d.datname IN ({})").format(
            sql.SQL(", ").join((map(sql.Literal, dbnames)))
        )
    with db.connect(ctx, instance) as cnx:
        with cnx.cursor(
            row_factory=psycopg.rows.class_row(interface.DetailedDatabase)
        ) as cur:
            cur.execute(db.query("database_list", where_clause=where_clause))
            return cur.fetchall()


@task("dropping '{name}' database from instance {instance}")
def drop(ctx: BaseContext, instance: "system.PostgreSQLInstance", name: str) -> None:
    """Drop a database from a primary instance.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.connect(ctx, instance, autocommit=True) as cnx:
        cnx.execute(db.query("database_drop", database=sql.Identifier(name)))


def exists(ctx: BaseContext, instance: "system.PostgreSQLInstance", name: str) -> bool:
    """Return True if named database exists in 'instance'.

    The instance should be running.
    """
    with db.connect(ctx, instance) as cnx:
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
    ctx: BaseContext,
    instance: "system.PostgreSQLInstance",
    database: interface.Database,
) -> None:
    """Create 'database' in 'instance'.

    The instance should be a running primary and the database should not exist already.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    options, args = options_and_args(database)
    with db.connect(ctx, instance, autocommit=True) as cnx:
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

    if database.extensions is not None:
        db.create_or_drop_extensions(
            ctx, instance, database.extensions, dbname=database.name
        )


@task("altering '{database.name}' database on instance {instance}")
def alter(
    ctx: BaseContext,
    instance: "system.PostgreSQLInstance",
    database: interface.Database,
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
    with db.connect(ctx, instance) as cnx:
        cnx.execute(
            db.query(
                "database_alter",
                database=sql.Identifier(database.name),
                options=options,
            ),
        )
        cnx.commit()

    if database.settings is not None:
        with db.connect(ctx, instance) as cnx:
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

    if database.extensions is not None:
        db.create_or_drop_extensions(
            ctx, instance, database.extensions, dbname=database.name
        )


def run(
    ctx: BaseContext,
    instance: "system.PostgreSQLInstance",
    sql_command: str,
    *,
    dbnames: Sequence[str] = (),
    exclude_dbnames: Sequence[str] = (),
    notice_handler: types.NoticeHandler = db.default_notice_handler,
) -> Dict[str, List[Dict[str, Any]]]:
    """Execute a SQL command on databases of `instance`.

    :param dbnames: restrict operation on databases with a name in this list.
    :param exclude_dbnames: exclude databases with a name in this list from
        the operation.
    :param notice_handler: a function to handle notice.

    :returns: a dict mapping database names to query results, if any.

    :raises psycopg.ProgrammingError: in case of unprocessable query.
    """
    result = {}
    for database in list(ctx, instance):
        if (
            dbnames and database.name not in dbnames
        ) or database.name in exclude_dbnames:
            continue
        with db.connect(ctx, instance, dbname=database.name, autocommit=True) as cnx:
            cnx.add_notice_handler(notice_handler)
            logger.info(
                'running "%s" on %s database of %s',
                sql_command,
                database.name,
                instance,
            )
            cur = cnx.execute(sql_command)
            if cur.statusmessage:
                logger.info(cur.statusmessage)
            if cur.description is not None:
                result[database.name] = cur.fetchall()
    return result


@task("backing up database '{dbname}' on instance {instance}")
def dump(ctx: BaseContext, instance: "system.PostgreSQLInstance", dbname: str) -> None:
    """dump a database of `instance` (logical backup)."""
    if not exists(ctx, instance, dbname):
        raise exceptions.DatabaseNotFound(dbname)
    postgresql_settings = ctx.settings.postgresql

    bindir = postgresql_settings.versions[instance.version].bindir
    conninfo = db.dsn(
        instance,
        postgresql_settings,
        dbname=dbname,
        user=ctx.settings.postgresql.surole.name,
    )

    date = (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )
    path = Path(str(postgresql_settings.dumps_directory).format(instance=instance))
    cmd = [
        c.format(bindir=bindir, path=path, conninfo=conninfo, dbname=dbname, date=date)
        for c in postgresql_settings.dump_command
    ]
    env = postgresql_settings.libpq_environ(ctx, instance)
    ctx.run(cmd, check=True, env=env)

    manifest = path / f"{dbname}_{date}.manifest"
    manifest.touch()
    manifest.write_text("# File created by pglift to keep track of database dumps\n")


def list_dumps(
    ctx: BaseContext, instance: "system.PostgreSQLInstance", dbnames: Sequence[str] = ()
) -> List[interface.DatabaseDump]:
    postgresql_settings = ctx.settings.postgresql
    path = Path(str(postgresql_settings.dumps_directory).format(instance=instance))
    dumps = (
        x.stem.rsplit("_", 1) for x in sorted(path.glob("*.manifest")) if x.is_file()
    )
    return [
        interface.DatabaseDump(dbname=dbname, date=date)
        for dbname, date in dumps
        if not dbnames or dbname in dbnames
    ]


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: "BaseContext", manifest: "interface.Instance", creating: bool
) -> None:
    if creating:
        instance = system.BaseInstance.get(manifest.name, manifest.version, ctx)
        dumps_directory = Path(
            str(ctx.settings.postgresql.dumps_directory).format(instance=instance)
        )
        dumps_directory.mkdir(parents=True, exist_ok=True)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: "BaseContext", instance: "system.Instance") -> None:

    dumps_directory = Path(
        str(ctx.settings.postgresql.dumps_directory).format(instance=instance)
    )
    if not dumps_directory.exists():
        return
    has_dumps = next(dumps_directory.iterdir(), None) is not None
    if not has_dumps or ctx.confirm(
        f"Confirm deletion of database dump(s) for instance {instance} ?",
        True,
    ):
        ctx.rmtree(dumps_directory)
