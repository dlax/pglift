import datetime
from typing import Optional

import pytest
from pydantic import SecretStr

from pglift import db, roles
from pglift.models import interface


def test_options_and_args():
    role = interface.Role(
        name="r",
        password="skret",
        inherit=False,
        login=True,
        connection_limit=2,
        validity=datetime.datetime(2024, 1, 1),
        in_roles=["pg_monitor"],
    )
    options, args = roles.options_and_args(role)

    SQL = db.sql.SQL
    Composed = db.sql.Composed
    Identifier = db.sql.Identifier
    Placeholder = db.sql.Placeholder
    assert options.seq == [
        SQL("NOINHERIT"),
        SQL(" "),
        SQL("LOGIN"),
        SQL(" "),
        Composed([SQL("PASSWORD"), SQL(" "), Placeholder("password")]),
        SQL(" "),
        Composed([SQL("VALID UNTIL"), SQL(" "), Placeholder("validity")]),
        SQL(" "),
        Composed([SQL("CONNECTION LIMIT"), SQL(" "), Placeholder("connection_limit")]),
        SQL(" "),
        Composed([SQL("IN ROLE"), SQL(" "), Composed([Identifier("pg_monitor")])]),
    ]

    assert args == {
        "connection_limit": 2,
        "password": "skret",
        "validity": "2024-01-01T00:00:00",
    }


class Role:
    def __init__(
        self, name: str, password: Optional[str] = None, pgpass: bool = False
    ) -> None:
        self.name = name
        self.password = SecretStr(password) if password is not None else None
        self.pgpass = pgpass


@pytest.fixture
def passfile(ctx):
    fpath = ctx.settings.postgresql.auth.passfile
    fpath.write_text("*:999:*:edgar:fbi\n")
    return fpath


def test_in_pgpass(ctx, instance, passfile):
    assert roles.in_pgpass(ctx, instance, Role("edgar"))
    assert not roles.in_pgpass(ctx, instance, Role("alice"))


@pytest.mark.parametrize(
    "role, pgpass",
    [
        (Role("alice"), "*:999:*:edgar:fbi\n"),
        (Role("bob", "secret"), "*:999:*:edgar:fbi\n"),
        (Role("charles", pgpass=True), "*:999:*:edgar:fbi\n"),
        (Role("danny", "sss", True), "*:999:*:danny:sss\n*:999:*:edgar:fbi\n"),
        (Role("edgar", "fbi", True), "*:999:*:edgar:fbi\n"),
        (Role("edgar", None, False), ""),
    ],
)
def test_set_pgpass_entry_for(ctx, instance, passfile, role, pgpass):
    roles.set_pgpass_entry_for(ctx, instance, role)
    assert passfile.read_text() == pgpass
