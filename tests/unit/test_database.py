import pytest

from pglift import databases, exceptions
from pglift.ctx import Context
from pglift.models.interface import Database
from pglift.models.system import Instance


def test_standby_database_create(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        databases.create(ctx, standby_instance, Database(name="test"))


def test_standby_database_alter(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        databases.alter(ctx, standby_instance, Database(name="test"))


def test_standby_database_drop(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        databases.drop(ctx, standby_instance, "test")
