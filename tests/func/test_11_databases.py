import datetime
import time
from typing import Iterator

import pytest

from pglift import databases, exceptions
from pglift import instance as instance_mod
from pglift.ctx import Context
from pglift.models import interface, system

from . import execute
from .conftest import DatabaseFactory, RoleFactory


@pytest.fixture(scope="module", autouse=True)
def instance_running(ctx: Context, instance: system.Instance) -> Iterator[None]:
    with instance_mod.running(ctx, instance):
        yield


def test_exists(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    assert not databases.exists(ctx, instance, "absent")
    database_factory("present")
    assert databases.exists(ctx, instance, "present")


def test_create(
    ctx: Context, instance: system.Instance, role_factory: RoleFactory
) -> None:
    database = interface.Database(name="db1")
    assert not databases.exists(ctx, instance, database.name)
    databases.create(ctx, instance, database)
    try:
        assert databases.describe(ctx, instance, database.name) == database.copy(
            update={"owner": "postgres"}
        )
    finally:
        # Drop database in order to avoid side effects in other tests.
        databases.drop(ctx, instance, "db1")

    role_factory("dba1")
    database = interface.Database(name="db2", owner="dba1")
    databases.create(ctx, instance, database)
    try:
        assert databases.describe(ctx, instance, database.name) == database
    finally:
        # Drop database in order to allow the role to be dropped in fixture.
        databases.drop(ctx, instance, database.name)


def test_apply(
    ctx: Context,
    instance: system.Instance,
    database_factory: DatabaseFactory,
    role_factory: RoleFactory,
) -> None:
    database = interface.Database(name="db2")
    assert not databases.exists(ctx, instance, database.name)
    databases.apply(ctx, instance, database)
    assert databases.exists(ctx, instance, database.name)

    database_factory("apply")
    database = interface.Database(name="apply")
    databases.apply(ctx, instance, database)
    assert databases.describe(ctx, instance, "apply").owner == "postgres"

    role_factory("dbapply")
    database = interface.Database(name="apply", owner="dbapply")
    databases.apply(ctx, instance, database)
    try:
        assert databases.describe(ctx, instance, "apply") == database
    finally:
        databases.drop(ctx, instance, "apply")

    database = interface.Database(name="db2", state="absent")
    assert databases.exists(ctx, instance, database.name)
    databases.apply(ctx, instance, database)
    assert not databases.exists(ctx, instance, database.name)


def test_describe(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    with pytest.raises(exceptions.DatabaseNotFound, match="absent"):
        databases.describe(ctx, instance, "absent")

    database_factory("describeme")
    database = databases.describe(ctx, instance, "describeme")
    assert database.name == "describeme"


def test_list(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    database_factory("db1")
    dbs = databases.list(ctx, instance)
    db1 = next(d for d in dbs if d.name == "db1").dict()
    db1.pop("size")
    db1["tablespace"].pop("size")
    assert db1 == {
        "acls": None,
        "collation": "C",
        "ctype": "C",
        "description": None,
        "encoding": "UTF8",
        "name": "db1",
        "owner": "postgres",
        "tablespace": {"location": "", "name": "pg_default"},
    }


def test_alter(
    ctx: Context,
    instance: system.Instance,
    database_factory: DatabaseFactory,
    role_factory: RoleFactory,
) -> None:
    database = interface.Database(name="alterme")
    with pytest.raises(exceptions.DatabaseNotFound, match="alter"):
        databases.alter(ctx, instance, database)

    database_factory("alterme")
    role_factory("alterdba")
    database = interface.Database(name="alterme", owner="alterdba")
    databases.alter(ctx, instance, database)
    assert databases.describe(ctx, instance, "alterme") == database

    database = interface.Database(name="alterme")
    databases.alter(ctx, instance, database)
    assert databases.describe(ctx, instance, "alterme") == database.copy(
        update={"owner": "postgres"}
    )


def test_drop(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    with pytest.raises(exceptions.DatabaseNotFound, match="absent"):
        databases.drop(ctx, instance, "absent")

    database_factory("dropme")
    databases.drop(ctx, instance, "dropme")
    assert not databases.exists(ctx, instance, "dropme")


def test_run_analyze(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    database_factory("test")

    def last_analyze() -> datetime.datetime:
        result = execute(
            ctx,
            instance,
            "SELECT MIN(last_analyze) m FROM pg_stat_all_tables WHERE last_analyze IS NOT NULL",
            dbname="test",
        )[0]["m"]
        assert isinstance(result, datetime.datetime), result
        return result

    databases.run(ctx, instance, "ANALYZE")
    previous = last_analyze()
    time.sleep(0.5)
    databases.run(ctx, instance, "ANALYZE")
    now = last_analyze()
    assert now > previous
    time.sleep(0.5)
    databases.run(ctx, instance, "ANALYZE", exclude_dbnames=["test"])
    assert last_analyze() == now


def test_run_output_notices(
    ctx: Context, instance: system.Instance, capsys: pytest.CaptureFixture[str]
) -> None:
    databases.run(
        ctx, instance, "DO $$ BEGIN RAISE NOTICE 'foo'; END $$", dbnames=["postgres"]
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "foo\n"
