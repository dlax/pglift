import pathlib
import re
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, Tuple

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

if TYPE_CHECKING:  # pragma: nocover
    from .model import Instance
    from .types import Role

QUERIES = pathlib.Path(__file__).parent / "queries.sql"


def query(name: str, **kwargs: str) -> sql.Composed:
    for qname, qstr in queries():
        if qname == name:
            kwargs = {k: sql.Identifier(v) for k, v in kwargs.items()}
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


def dsn(instance: "Instance", role: "Role", **kwargs: Any) -> str:
    for badarg in ("port", "user", "password", "passfile", "host"):
        if badarg in kwargs:
            raise TypeError(f"unexpected '{badarg}' argument")

    kwargs["port"] = instance.port
    kwargs["user"] = role.name
    config = instance.config()
    if config.unix_socket_directories:
        kwargs["host"] = config.unix_socket_directories
    passfile = instance.settings.postgresql.auth.passfile
    if role.pgpass and passfile.exists():
        kwargs["passfile"] = str(passfile)
    elif role.password:
        kwargs["password"] = role.password.get_secret_value()

    assert "dsn" not in kwargs
    return psycopg2.extensions.make_dsn(**kwargs)  # type: ignore[no-any-return]


@contextmanager
def connect(
    instance: "Instance",
    role: "Role",
    *,
    dbname: str = "postgres",
    autocommit: bool = False,
) -> Iterator[psycopg2.extensions.connection]:
    """Connect to specified database of `instance` with `role`."""
    conninfo = dsn(instance, role, dbname=dbname)
    conn = psycopg2.connect(conninfo)
    if autocommit:
        conn.autocommit = True
        yield conn
        return

    with conn as conn:
        yield conn
