import logging
import pathlib
import re
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, List, Sequence, Tuple

import psycopg.conninfo
import psycopg.errors
import psycopg.rows
from psycopg import sql

from .settings import EXTENSIONS_CONFIG
from .types import Extension

if TYPE_CHECKING:  # pragma: nocover
    from .ctx import BaseContext
    from .models.system import PostgreSQLInstance, Standby
    from .settings import PostgreSQLSettings

QUERIES = pathlib.Path(__file__).parent / "queries.sql"
logger = logging.getLogger(__name__)


def query(name: str, **kwargs: sql.Composable) -> sql.Composed:
    for qname, qstr in queries():
        if qname == name:
            return sql.SQL(qstr).format(**kwargs)
    raise ValueError(name)


def queries() -> Iterator[Tuple[str, str]]:
    content = QUERIES.read_text()
    for block in re.split("-- name:", content):
        block = block.strip()
        if not block:
            continue
        qname, query = block.split("\n", 1)
        yield qname.strip(), query.strip()


def dsn(
    instance: "PostgreSQLInstance", settings: "PostgreSQLSettings", **kwargs: Any
) -> str:
    for badarg in ("port", "passfile", "host"):
        if badarg in kwargs:
            raise TypeError(f"unexpected '{badarg}' argument")

    kwargs["port"] = instance.port
    config = instance.config()
    if config.unix_socket_directories:
        kwargs["host"] = config.unix_socket_directories
    passfile = settings.auth.passfile
    if passfile.exists():
        kwargs["passfile"] = str(passfile)

    assert "dsn" not in kwargs
    return psycopg.conninfo.make_conninfo(**kwargs)


def obfuscate_conninfo(conninfo: str, **kwargs: Any) -> str:
    """Return an obfuscated connection string with password hidden.

    >>> obfuscate_conninfo("user=postgres password=foo")
    'user=postgres password=********'
    >>> obfuscate_conninfo("user=postgres", password="secret")
    'user=postgres password=********'
    >>> obfuscate_conninfo("port=5444")
    'port=5444'
    """
    params = psycopg.conninfo.conninfo_to_dict(conninfo, **kwargs)
    if "password" in params:
        params["password"] = "*" * 8
    return psycopg.conninfo.make_conninfo(**params)


@contextmanager
def connect_dsn(
    conninfo: str, autocommit: bool = False, **kwargs: Any
) -> Iterator[psycopg.Connection[psycopg.rows.DictRow]]:
    """Connect to specified database of `conninfo` dsn string"""
    logger.debug(
        "connecting to PostgreSQL instance with: %s",
        obfuscate_conninfo(conninfo, **kwargs),
    )
    conn = psycopg.connect(conninfo, row_factory=psycopg.rows.dict_row, **kwargs)
    if autocommit:
        conn.autocommit = True
        yield conn
        return

    with conn as conn:
        yield conn


@contextmanager
def connect(
    instance: "PostgreSQLInstance",
    settings: "PostgreSQLSettings",
    *,
    dbname: str = "postgres",
    autocommit: bool = False,
    **kwargs: Any,
) -> Iterator[psycopg.Connection[psycopg.rows.DictRow]]:
    """Connect to specified database of `instance` with `role`."""
    conninfo = dsn(instance, settings, dbname=dbname, **kwargs)
    with connect_dsn(conninfo, autocommit=autocommit) as cnx:
        yield cnx


@contextmanager
def superuser_connect(
    ctx: "BaseContext", instance: "PostgreSQLInstance", **kwargs: Any
) -> Iterator[psycopg.Connection[psycopg.rows.DictRow]]:
    if "user" in kwargs:
        raise TypeError("unexpected 'user' argument")
    postgresql_settings = ctx.settings.postgresql
    kwargs["user"] = postgresql_settings.surole.name
    if "password" not in kwargs:
        kwargs["password"] = postgresql_settings.libpq_environ(ctx, instance).get(
            "PGPASSWORD"
        )
    try:
        with connect(instance, postgresql_settings, **kwargs) as cnx:
            yield cnx
    except psycopg.OperationalError:
        if kwargs.get("password"):
            raise
        password = ctx.prompt(
            f"Password for user {postgresql_settings.surole.name}", hide_input=True
        )
        if not password:
            raise
        kwargs["password"] = password
        with connect(instance, postgresql_settings, **kwargs) as cnx:
            yield cnx


@contextmanager
def primary_connect(
    standby: "Standby", *, dbname: str = "template1", **kwargs: Any
) -> Iterator[psycopg.Connection[psycopg.rows.DictRow]]:
    """Connect to the primary of standby."""
    if standby.password:
        kwargs.setdefault("password", standby.password.get_secret_value())
    with connect_dsn(standby.for_, dbname=dbname, **kwargs) as cnx:
        yield cnx


def default_notice_handler(diag: psycopg.errors.Diagnostic) -> None:
    if diag.message_primary is not None:
        sys.stderr.write(diag.message_primary + "\n")


def installed_extensions(
    ctx: "BaseContext",
    instance: "PostgreSQLInstance",
    dbname: str = "postgres",
) -> List[Extension]:
    """Return list of extensions installed in database using CREATE EXTENSION"""
    with superuser_connect(ctx, instance, dbname=dbname) as cnx:
        return [
            Extension(r["extname"])
            for r in cnx.execute(
                "SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname"
            )
        ]


def create_or_drop_extensions(
    ctx: "BaseContext",
    instance: "PostgreSQLInstance",
    extensions: Sequence[Extension],
    dbname: str = "postgres",
) -> None:
    """Create or drop extensions from 'dbname' database on instance.

    We compare what is already installed to what is set in extensions.
    The 'plpgsql' extension will not be dropped because it is meant to be installed
    by default.
    """
    installed = installed_extensions(ctx, instance, dbname=dbname)
    with superuser_connect(ctx, instance, autocommit=True, dbname=dbname) as cnx:
        extensions = [e for e in extensions if EXTENSIONS_CONFIG[e][1]]
        to_add = set(extensions) - set(installed)
        to_remove = set(installed) - set(extensions)
        for extension in sorted(to_add):
            cnx.execute(
                psycopg.sql.SQL(
                    "CREATE EXTENSION IF NOT EXISTS {extension} CASCADE"
                ).format(extension=psycopg.sql.Identifier(extension))
            )
        for extension in sorted(to_remove):
            cnx.execute(
                psycopg.sql.SQL("DROP EXTENSION IF EXISTS {extension} CASCADE").format(
                    extension=psycopg.sql.Identifier(extension)
                )
            )
