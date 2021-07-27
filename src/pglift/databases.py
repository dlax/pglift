from . import db, exceptions
from .ctx import BaseContext
from .models import interface
from .models.system import Instance


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
    # TODO: implement update()


def describe(ctx: BaseContext, instance: Instance, name: str) -> interface.Database:
    """Return a database described as a manifest.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    database = interface.Database(name=name)
    return database


def drop(ctx: BaseContext, instance: Instance, name: str) -> None:
    """Drop a database from instance.

    :raises ~pglift.exceptions.DatabaseNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.DatabaseNotFound(name)
    with db.connect(instance, ctx.settings.postgresql.surole, autocommit=True) as cnx:
        with cnx.cursor() as cur:
            cur.execute(db.query("database_drop", database=db.sql.Identifier(name)))


def exists(ctx: BaseContext, instance: Instance, name: str) -> bool:
    """Return True if named database exists in 'instance'.

    The instance should be running.
    """
    with db.connect(instance, ctx.settings.postgresql.surole) as cnx:
        with cnx.cursor() as cur:
            cur.execute(db.query("database_exists"), {"database": name})
            return cur.rowcount == 1  # type: ignore[no-any-return]


def create(ctx: BaseContext, instance: Instance, database: interface.Database) -> None:
    """Create 'database' in 'instance'.

    The instance should be running and the database should not exist already.
    """
    with db.connect(instance, ctx.settings.postgresql.surole, autocommit=True) as cnx:
        query = db.query("database_create", database=db.sql.Identifier(database.name))
        with cnx.cursor() as cur:
            cur.execute(query)
