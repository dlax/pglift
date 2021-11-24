import json
from typing import Optional
from unittest.mock import MagicMock, patch

import psycopg2.extras
import pytest
from psycopg2 import sql
from pydantic import SecretStr

from pglift import db


def test_queries(datadir, regen_test_data):
    actual = dict(db.queries())
    fpath = datadir / "queries.json"
    if regen_test_data:
        with fpath.open("w") as f:
            json.dump(actual, f, indent=2, sort_keys=True)
    expected = json.loads(fpath.read_text())
    assert actual == expected


def test_query():
    query = db.query("role_alter_password", username=sql.Identifier("bob"))
    qs = "".join(q.string for q in query.seq)
    assert qs == "ALTER ROLE bob PASSWORD %(password)s;"


@pytest.mark.parametrize(
    "rolspec, expected",
    [
        (
            ("bob", None),
            "dbname=mydb sslmode=off port=999 user=bob host=/socks passfile={passfile}",
        ),
        (
            ("alice", "s3kret"),
            "dbname=mydb sslmode=off port=999 user=alice host=/socks passfile={passfile} password=s3kret",
        ),
    ],
)
def test_dsn(settings, instance, rolspec, expected):
    passfile = settings.postgresql.auth.passfile

    class MyRole:
        def __init__(self, username: str, password: Optional[str]) -> None:
            self.name = username
            self.password = SecretStr(password) if password is not None else None
            self.pgpass = False

    conninfo = db.dsn(instance, MyRole(*rolspec), dbname="mydb", sslmode="off")
    assert conninfo == expected.format(passfile=passfile)


def test_dsn_badarg(instance):
    role = MagicMock()
    with pytest.raises(TypeError, match="unexpected 'port' argument"):
        db.dsn(instance, role, port=123)


def test_connect(instance, settings):
    role = MagicMock()
    role.configure_mock(name="dba", password=None, pgpass=False)
    with patch("psycopg2.connect") as connect:
        cnx = db.connect(instance, role)
        assert not connect.called
        with cnx:
            pass
    passfile = settings.postgresql.auth.passfile
    assert passfile.exists()
    connect.assert_called_once_with(
        f"dbname=postgres port=999 user=dba host=/socks passfile={passfile}",
        connection_factory=psycopg2.extras.DictConnection,
    )
