from pathlib import Path
from typing import Optional

import pytest
from pydantic import SecretStr

from pglift import exceptions, roles
from pglift.ctx import Context
from pglift.models import interface
from pglift.models.system import Instance


class Role(interface.Role):
    def __init__(
        self, name: str, password: Optional[str] = None, pgpass: bool = False
    ) -> None:
        super().__init__(
            name=name,
            password=SecretStr(password) if password is not None else None,
            pgpass=pgpass,
        )


@pytest.fixture
def passfile(ctx: Context) -> Path:
    fpath = ctx.settings.postgresql.auth.passfile
    fpath.write_text("*:999:*:edgar:fbi\n")
    return fpath


def test_in_pgpass(ctx: Context, instance: Instance, passfile: Path) -> None:
    assert roles.in_pgpass(ctx, instance, "edgar")
    assert not roles.in_pgpass(ctx, instance, "alice")


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
def test_set_pgpass_entry_for(
    ctx: Context, instance: Instance, passfile: Path, role: Role, pgpass: str
) -> None:
    roles.set_pgpass_entry_for(ctx, instance, role)
    assert passfile.read_text() == pgpass


def test_standby_role_create(ctx: Context, standby_instance: Instance) -> None:
    role = Role("alice")
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        roles.create(ctx, standby_instance, role)


def test_standby_role_drop(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        roles.drop(ctx, standby_instance, "alice")


def test_standby_role_alter(ctx: Context, standby_instance: Instance) -> None:
    role = Role("alice")
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        roles.alter(ctx, standby_instance, role)


def test_standby_set_password_for(ctx: Context, standby_instance: Instance) -> None:
    role = Role("alice")
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        roles.set_password_for(ctx, standby_instance, role)
