import datetime
import fnmatch
import logging
import time
from pathlib import Path
from typing import Iterator

import pytest

from pglift import databases, exceptions, instances
from pglift.ctx import Context
from pglift.models import interface, system

from . import execute
from .conftest import DatabaseFactory, RoleFactory


@pytest.fixture(scope="module", autouse=True)
def instance_running(ctx: Context, instance: system.Instance) -> Iterator[None]:
    with instances.running(ctx, instance):
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
        assert databases.get(ctx, instance, database.name) == database.copy(
            update={"owner": "postgres"}
        )
    finally:
        # Drop database in order to avoid side effects in other tests.
        databases.drop(ctx, instance, "db1")

    role_factory("dba1")
    database = interface.Database(name="db2", owner="dba1")
    databases.create(ctx, instance, database)
    try:
        assert databases.get(ctx, instance, database.name) == database
    finally:
        # Drop database in order to allow the role to be dropped in fixture.
        databases.drop(ctx, instance, database.name)


def test_apply(
    ctx: Context,
    instance: system.Instance,
    database_factory: DatabaseFactory,
    role_factory: RoleFactory,
) -> None:
    database = interface.Database(
        name="db2",
        settings={"work_mem": "1MB"},
        extensions=[interface.Extension.unaccent],
    )
    assert not databases.exists(ctx, instance, database.name)
    assert databases.apply(ctx, instance, database)
    db = databases.get(ctx, instance, database.name)
    assert db.settings == {"work_mem": "1MB"}
    assert db.extensions == [interface.Extension.unaccent]
    assert databases.apply(ctx, instance, database) is False  # no-op

    database_factory("apply")
    database = interface.Database(name="apply")
    assert databases.apply(ctx, instance, database) is False  # no-op
    assert databases.get(ctx, instance, "apply").owner == "postgres"

    role_factory("dbapply")
    database = interface.Database(name="apply", owner="dbapply")
    assert databases.apply(ctx, instance, database)
    try:
        assert databases.get(ctx, instance, "apply") == database
    finally:
        databases.drop(ctx, instance, "apply")

    database = interface.Database(name="db2", state="absent")
    assert databases.exists(ctx, instance, database.name)
    assert databases.apply(ctx, instance, database) is None
    assert not databases.exists(ctx, instance, database.name)


def test_get(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    with pytest.raises(exceptions.DatabaseNotFound, match="absent"):
        databases.get(ctx, instance, "absent")

    database_factory("describeme")
    execute(
        ctx, instance, "ALTER DATABASE describeme SET work_mem TO '3MB'", fetch=False
    )
    execute(
        ctx, instance, "CREATE EXTENSION unaccent", fetch=False, dbname="describeme"
    )
    database = databases.get(ctx, instance, "describeme")
    assert database.name == "describeme"
    assert database.settings == {"work_mem": "3MB"}
    assert database.extensions == [interface.Extension.unaccent]


def test_list(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    database_factory("db1")
    database_factory("db2")
    dbs = databases.list(ctx, instance)
    dbnames = [d.name for d in dbs]
    assert "db2" in dbnames
    dbs = databases.list(ctx, instance, dbnames=("db1",))
    dbnames = [d.name for d in dbs]
    assert "db2" not in dbnames
    assert len(dbs) == 1
    db1 = next(d for d in dbs).dict()
    db1.pop("size")
    db1["tablespace"].pop("size")
    assert db1 == {
        "acls": [],
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
    database = interface.Database(name="alterme", owner="postgres")
    with pytest.raises(exceptions.DatabaseNotFound, match="alter"):
        databases.alter(ctx, instance, database)

    database_factory("alterme")
    execute(ctx, instance, "ALTER DATABASE alterme SET work_mem TO '3MB'", fetch=False)
    execute(ctx, instance, "CREATE EXTENSION unaccent", fetch=False, dbname="alterme")
    assert databases.get(ctx, instance, "alterme") == database.copy(
        update={
            "settings": {"work_mem": "3MB"},
            "extensions": [interface.Extension.unaccent],
        }
    )
    role_factory("alterdba")
    database = interface.Database(
        name="alterme",
        owner="alterdba",
        settings={"work_mem": None, "maintenance_work_mem": "9MB"},
        extensions=[interface.Extension.pg_stat_statements],
    )
    databases.alter(ctx, instance, database)
    assert databases.get(ctx, instance, "alterme") == database.copy(
        update={
            "settings": {"maintenance_work_mem": "9MB"},
            "extensions": [interface.Extension.pg_stat_statements],
        }
    )

    database = interface.Database(name="alterme", settings={}, extensions=[])
    databases.alter(ctx, instance, database)
    assert databases.get(ctx, instance, "alterme") == database.copy(
        update={"owner": "postgres", "settings": None, "extensions": []}
    )


def test_drop(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    with pytest.raises(exceptions.DatabaseNotFound, match="absent"):
        databases.drop(ctx, instance, "absent")

    database_factory("dropme")
    databases.drop(ctx, instance, "dropme")
    assert not databases.exists(ctx, instance, "dropme")


def test_run(
    ctx: Context,
    instance: system.Instance,
    database_factory: DatabaseFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database_factory("test")
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="pglift"):
        result_run = databases.run(
            ctx,
            instance,
            "CREATE TABLE persons AS (SELECT 'bob' AS name)",
            dbnames=["test"],
        )
    assert "CREATE TABLE persons AS (SELECT 'bob' AS name)" in caplog.records[0].message
    assert "SELECT 1" in caplog.records[1].message
    assert not result_run
    result = execute(ctx, instance, "SELECT * FROM persons", dbname="test")
    assert result == [{"name": "bob"}]
    result_run = databases.run(
        ctx,
        instance,
        "SELECT * from persons",
        dbnames=["test"],
    )
    assert result_run == {"test": [{"name": "bob"}]}


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


def test_dump(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    with pytest.raises(exceptions.DatabaseNotFound, match="absent"):
        databases.dump(ctx, instance, "absent")
    database_factory("dbtodump")
    databases.dump(ctx, instance, "dbtodump")
    directory = Path(
        str(ctx.settings.postgresql.dumps_directory).format(instance=instance)
    )
    assert directory.exists()
    (dumpfile, manifest) = sorted(directory.iterdir())
    assert fnmatch.fnmatch(str(dumpfile), "*dbtodump_*.dump"), dumpfile
    assert fnmatch.fnmatch(str(manifest), "*dbtodump_*.manifest"), manifest


def test_list_dumps(
    ctx: Context, instance: system.Instance, database_factory: DatabaseFactory
) -> None:
    database_factory("dbtodump")
    databases.dump(ctx, instance, "dbtodump")
    dumps = databases.list_dumps(ctx, instance)
    dbnames = [d.dbname for d in dumps]
    assert "dbtodump" in dbnames

    dumps = databases.list_dumps(ctx, instance, dbnames=("dbtodump",))
    dbnames = [d.dbname for d in dumps]
    assert "dbtodump" in dbnames

    dumps = databases.list_dumps(ctx, instance, dbnames=("otherdb",))
    assert dumps == []
