import pytest

from pglift import databases, exceptions
from pglift import instance as instance_mod
from pglift.models import interface

from . import execute


@pytest.fixture(scope="module", autouse=True)
def instance_running(ctx, instance):
    with instance_mod.running(ctx, instance):
        yield


@pytest.fixture(scope="module")
def database_factory(ctx, instance):
    datnames = set()

    def factory(name: str) -> None:
        if name in datnames:
            raise ValueError(f"'{name}' name already taken")
        execute(
            ctx,
            instance,
            f"CREATE DATABASE {name}",
            fetch=False,
            autocommit=True,
        )
        datnames.add(name)

    yield factory

    for name in datnames:
        execute(
            ctx,
            instance,
            f"DROP DATABASE IF EXISTS {name}",
            fetch=False,
            autocommit=True,
        )


def test_exists(ctx, instance, database_factory):
    assert not databases.exists(ctx, instance, "absent")
    database_factory("present")
    assert databases.exists(ctx, instance, "present")


def test_create(ctx, instance):
    database = interface.Database(name="db1")
    assert not databases.exists(ctx, instance, database.name)
    databases.create(ctx, instance, database)
    assert databases.exists(ctx, instance, database.name)


def test_apply(ctx, instance, database_factory):
    database = interface.Database(name="db2")
    assert not databases.exists(ctx, instance, database.name)
    databases.apply(ctx, instance, database)
    assert databases.exists(ctx, instance, database.name)

    database_factory("apply")
    database = interface.Database(name="apply")
    databases.apply(ctx, instance, database)

    database = interface.Database(name="db2", state="absent")
    assert databases.exists(ctx, instance, database.name)
    databases.apply(ctx, instance, database)
    assert not databases.exists(ctx, instance, database.name)


def test_describe(ctx, instance, database_factory):
    with pytest.raises(exceptions.DatabaseNotFound, match="absent"):
        databases.describe(ctx, instance, "absent")

    database_factory("describeme")
    database = databases.describe(ctx, instance, "describeme")
    assert database.name == "describeme"


def test_drop(ctx, instance, database_factory):
    with pytest.raises(exceptions.DatabaseNotFound, match="absent"):
        databases.drop(ctx, instance, "absent")

    database_factory("dropme")
    databases.drop(ctx, instance, "dropme")
    assert not databases.exists(ctx, instance, "dropme")
