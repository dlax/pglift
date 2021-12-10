import json
from pathlib import Path
from typing import Dict
from unittest.mock import patch

import psycopg2.extras
import pytest
from psycopg2 import sql

from pglift import db
from pglift.models.system import Instance
from pglift.settings import Settings


def test_queries(datadir: Path, regen_test_data: bool) -> None:
    actual = dict(db.queries())
    fpath = datadir / "queries.json"
    if regen_test_data:
        with fpath.open("w") as f:
            json.dump(actual, f, indent=2, sort_keys=True)
    expected = json.loads(fpath.read_text())
    assert actual == expected


def test_query() -> None:
    query = db.query("role_alter_password", username=sql.Identifier("bob"))
    qs = "".join(q.string for q in query.seq)
    assert qs == "ALTER ROLE bob PASSWORD %(password)s;"


@pytest.mark.parametrize(
    "connargs, expected",
    [
        (
            {"user": "bob"},
            "dbname=mydb sslmode=off user=bob port=999 host=/socks passfile={passfile}",
        ),
        (
            {"user": "alice", "password": "s3kret"},
            "dbname=mydb sslmode=off user=alice password=s3kret port=999 host=/socks passfile={passfile}",
        ),
    ],
)
def test_dsn(
    settings: Settings, instance: Instance, connargs: Dict[str, str], expected: str
) -> None:
    passfile = settings.postgresql.auth.passfile
    conninfo = db.dsn(instance, dbname="mydb", sslmode="off", **connargs)
    assert conninfo == expected.format(passfile=passfile)


def test_dsn_badarg(instance: Instance) -> None:
    with pytest.raises(TypeError, match="unexpected 'port' argument"):
        db.dsn(instance, port=123)


def test_connect(instance: Instance, settings: Settings) -> None:
    with patch("psycopg2.connect") as connect:
        cnx = db.connect(instance, user="dba")
        assert not connect.called
        with cnx:
            pass
    passfile = settings.postgresql.auth.passfile
    assert passfile.exists()
    connect.assert_called_once_with(
        f"dbname=postgres user=dba port=999 host=/socks passfile={passfile}",
        connection_factory=psycopg2.extras.DictConnection,
    )
