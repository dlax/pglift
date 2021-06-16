import functools
import pathlib
from typing import Optional, Union

import pytest
from pydantic import SecretStr

from pglift import exceptions
from pglift import instance as instance_mod
from pglift import roles, types
from pglift.models import interface

from . import execute, reconfigure_instance


@pytest.fixture(scope="module", autouse=True)
def instance_running(ctx, instance):
    with instance_mod.running(ctx, instance):
        yield


@pytest.fixture(scope="module")
def role_factory(ctx, instance):
    rolnames = set()

    def factory(name: str) -> None:
        if name in rolnames:
            raise ValueError(f"'{name}' name already taken")
        execute(ctx, instance, f"CREATE ROLE {name}", fetch=False)
        rolnames.add(name)

    yield factory

    for name in rolnames:
        execute(ctx, instance, f"DROP ROLE IF EXISTS {name}", fetch=False)


def test_exists(ctx, instance, role_factory):
    assert not roles.exists(ctx, instance, "absent")
    role_factory("present")
    assert roles.exists(ctx, instance, "present")


def test_create(ctx, instance, role_factory):
    role = interface.Role(name="nopassword")
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert not roles.has_password(ctx, instance, role)

    role = interface.Role(name="password", password="scret")
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert roles.has_password(ctx, instance, role)


def role_in_pgpass(
    passfile: pathlib.Path,
    role: types.Role,
    *,
    port: Optional[Union[int, str]] = None,
) -> bool:
    password = ""
    if role.password:
        password = role.password.get_secret_value()
    parts = [role.name, password]
    if port is not None:
        parts = [str(port), "*"] + parts
    pattern = ":".join(parts)
    with passfile.open() as f:
        for line in f:
            if pattern in line:
                return True
    return False


def test_apply(ctx, instance):
    rolname = "applyme"
    _role_in_pgpass = functools.partial(
        role_in_pgpass, ctx.settings.postgresql.auth.passfile
    )

    role = interface.Role(name=rolname)
    assert not roles.exists(ctx, instance, role.name)
    roles.apply(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert not roles.has_password(ctx, instance, role)
    assert not _role_in_pgpass(role)

    role = interface.Role(name=rolname, password=SecretStr("passw0rd"))
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert not _role_in_pgpass(role)

    role = interface.Role(name=rolname, password=SecretStr("passw0rd"), pgpass=True)
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert _role_in_pgpass(role)

    role = interface.Role(
        name=rolname, password=SecretStr("passw0rd_changed"), pgpass=True
    )
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert _role_in_pgpass(role)

    role = interface.Role(name=rolname, pgpass=False)
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert not _role_in_pgpass(role)


def test_describe(ctx, instance, role_factory):
    with pytest.raises(exceptions.RoleNotFound, match="absent"):
        roles.describe(ctx, instance, "absent")

    postgres = roles.describe(ctx, instance, "postgres")
    assert postgres is not None
    surole = ctx.settings.postgresql.surole
    assert postgres.name == "postgres"
    if surole.password:
        assert postgres.password is not None
    if surole.pgpass:
        assert postgres.pgpass is not None


def test_drop(ctx, instance, role_factory):
    with pytest.raises(exceptions.RoleNotFound, match="dropping_absent"):
        roles.drop(ctx, instance, "dropping_absent")
    role_factory("dropme")
    roles.drop(ctx, instance, "dropme")
    assert not roles.exists(ctx, instance, "dropme")


def test_instance_port_changed(ctx, instance, tmp_port_factory):
    """Check that change of instance port is reflected in password file
    entries.
    """
    role1, role2, role3 = (
        interface.Role(name="r1", password="1", pgpass=True),
        interface.Role(name="r2", password="2", pgpass=True),
        interface.Role(name="r3", pgpass=False),
    )
    surole = ctx.settings.postgresql.surole
    roles.apply(ctx, instance, role1)
    roles.apply(ctx, instance, role2)
    roles.apply(ctx, instance, role3)
    port = instance.port
    passfile = ctx.settings.postgresql.auth.passfile
    assert role_in_pgpass(passfile, role1, port=port)
    assert role_in_pgpass(passfile, role2, port=port)
    assert not role_in_pgpass(passfile, role3)
    if surole.pgpass:
        assert role_in_pgpass(passfile, surole, port=port)
    newport = next(tmp_port_factory)
    with reconfigure_instance(ctx, instance, port=newport):
        assert not role_in_pgpass(passfile, role1, port=port)
        assert role_in_pgpass(passfile, role1, port=newport)
        assert not role_in_pgpass(passfile, role2, port=port)
        assert role_in_pgpass(passfile, role2, port=newport)
        assert not role_in_pgpass(passfile, role3)
        if surole.pgpass:
            assert not role_in_pgpass(passfile, surole, port=port)
            assert role_in_pgpass(passfile, surole, port=newport)
