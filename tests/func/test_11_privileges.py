from typing import Iterator

import pytest

from pglift import instance as instance_mod
from pglift import privileges
from pglift.ctx import Context
from pglift.models import system
from pglift.models.interface import Privilege

from . import execute
from .conftest import DatabaseFactory, RoleFactory


@pytest.fixture(scope="module", autouse=True)
def instance_running(ctx: Context, instance: system.Instance) -> Iterator[None]:
    with instance_mod.running(ctx, instance):
        yield


@pytest.fixture(autouse=True)
def roles_and_privileges(
    ctx: Context,
    instance: system.Instance,
    role_factory: RoleFactory,
    database_factory: DatabaseFactory,
) -> None:
    role_factory("rol1")
    role_factory("rol2")
    database_factory("db1")
    database_factory("db2")
    execute(
        ctx,
        instance,
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO rol1",
        fetch=False,
        autocommit=True,
        dbname="db1",
    )
    execute(
        ctx,
        instance,
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO rol2",
        fetch=False,
        autocommit=True,
        dbname="db2",
    )


def test_get(ctx: Context, instance: system.Instance) -> None:
    expected = [
        Privilege(
            database="db1",
            schema="public",
            role="rol1",
            object_type="TABLE",
            privileges=[
                "DELETE",
                "INSERT",
                "REFERENCES",
                "SELECT",
                "TRIGGER",
                "TRUNCATE",
                "UPDATE",
            ],
        ),
        Privilege(
            database="db2",
            schema="public",
            role="rol2",
            object_type="FUNCTION",
            privileges=["EXECUTE"],
        ),
    ]
    prvlgs = privileges.get(ctx, instance)
    assert prvlgs == expected
    assert privileges.get(ctx, instance, databases=["db1"], roles=["rol2"]) == []
    assert (
        privileges.get(ctx, instance, databases=["db2"], roles=["rol2"])
        == expected[-1:]
    )
