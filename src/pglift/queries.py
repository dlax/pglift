import pathlib
import re
from typing import Iterator, Tuple

from psycopg2 import sql

QUERIES = pathlib.Path(__file__).parent / "queries.sql"


def get(name: str, **kwargs: str) -> sql.Composed:
    for qname, query in iter():
        if qname == name:
            kwargs = {k: sql.Identifier(v) for k, v in kwargs.items()}
            return sql.SQL(query).format(**kwargs)
    raise ValueError(name)


def iter() -> Iterator[Tuple[str, str]]:
    content = QUERIES.read_text()
    for block in re.split("-- name:", content):
        block = block.strip()
        if not block:
            continue
        qname, query = block.split("\n", 1)
        yield qname.strip(), query.strip()
