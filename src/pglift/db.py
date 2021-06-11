import pathlib
import re
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Tuple

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

if TYPE_CHECKING:  # pragma: nocover
    from .model import Instance
    from .settings import Role

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


@contextmanager
def connect(
    instance: "Instance", role: "Role", *, dbname: str = "postgres"
) -> Iterator[psycopg2.extensions.connection]:
    """Connect to specified database of `instance` with `role`."""
    config = instance.config()
    assert config is not None
    connargs = {
        "port": config.port,
        "dbname": "postgres",
        "user": role.name,
    }
    if config.unix_socket_directories:
        connargs["host"] = config.unix_socket_directories
    passfile = instance.settings.postgresql.auth.passfile
    if role.pgpass and passfile.exists():
        connargs["passfile"] = str(passfile)
    elif role.password:
        connargs["password"] = role.password.get_secret_value()
    with psycopg2.connect(**connargs) as conn:
        yield conn
