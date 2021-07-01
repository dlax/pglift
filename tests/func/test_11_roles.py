import pytest
from pydantic import SecretStr

from pglift import exceptions
from pglift import instance as instance_mod
from pglift import manifest, roles, types

from . import execute


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
    role = manifest.Role(name="nopassword")
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert not roles.has_password(ctx, instance, role)

    role = manifest.Role(name="password", password="scret")
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert roles.has_password(ctx, instance, role)


def test_apply(ctx, instance):
    rolname = "applyme"

    def role_in_pgpass(role: types.Role) -> bool:
        if role.password:
            pattern = f":{role.name}:{role.password.get_secret_value()}"
        else:
            pattern = f":{role.name}:"
        with ctx.settings.postgresql.auth.passfile.open() as f:
            for line in f:
                if pattern in line:
                    return True
        return False

    role = manifest.Role(name=rolname)
    assert not roles.exists(ctx, instance, role.name)
    roles.apply(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert not roles.has_password(ctx, instance, role)
    assert not role_in_pgpass(role)

    role = manifest.Role(name=rolname, password=SecretStr("passw0rd"))
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert not role_in_pgpass(role)

    role = manifest.Role(name=rolname, password=SecretStr("passw0rd"), pgpass=True)
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert role_in_pgpass(role)

    role = manifest.Role(
        name=rolname, password=SecretStr("passw0rd_changed"), pgpass=True
    )
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert role_in_pgpass(role)

    role = manifest.Role(name=rolname, pgpass=False)
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert not role_in_pgpass(role)


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
