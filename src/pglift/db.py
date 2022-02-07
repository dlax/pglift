import pathlib
import re
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ContextManager, Iterator, Tuple

import psycopg.conninfo
import psycopg.errors
import psycopg.rows
from psycopg import sql

if TYPE_CHECKING:  # pragma: nocover
    from .ctx import BaseContext
    from .models.system import PostgreSQLInstance
    from .settings import PostgreSQLSettings

QUERIES = pathlib.Path(__file__).parent / "queries.sql"


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


@contextmanager
def connect_dsn(
    conninfo: str, autocommit: bool = False, **kwargs: Any
) -> Iterator[psycopg.Connection[psycopg.rows.DictRow]]:
    """Connect to specified database of `conninfo` dsn string"""
    conn = psycopg.connect(conninfo, row_factory=psycopg.rows.dict_row, **kwargs)
    if autocommit:
        conn.autocommit = True
        yield conn
        return

    with conn as conn:
        yield conn


def connect(
    instance: "PostgreSQLInstance",
    settings: "PostgreSQLSettings",
    *,
    dbname: str = "postgres",
    autocommit: bool = False,
    **kwargs: Any,
) -> ContextManager[psycopg.Connection[psycopg.rows.DictRow]]:
    """Connect to specified database of `instance` with `role`."""
    conninfo = dsn(instance, settings, dbname=dbname, **kwargs)
    return connect_dsn(conninfo, autocommit=autocommit)


def superuser_connect(
    ctx: "BaseContext", instance: "PostgreSQLInstance", **kwargs: Any
) -> ContextManager[psycopg.Connection[psycopg.rows.DictRow]]:
    if "user" in kwargs:
        raise TypeError("unexpected 'user' argument")
    kwargs["user"] = ctx.settings.postgresql.surole.name
    if "password" not in kwargs:
        kwargs["password"] = ctx.libpq_environ().get("PGPASSWORD")
    return connect(instance, ctx.settings.postgresql, **kwargs)


def default_notice_handler(diag: psycopg.errors.Diagnostic) -> None:
    if diag.message_primary is not None:
        sys.stderr.write(diag.message_primary + "\n")
