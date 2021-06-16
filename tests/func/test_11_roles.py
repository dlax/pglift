import pytest

from pglift import instance as instance_mod
from pglift import manifest, roles

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
    def has_password(rolname: str) -> bool:
        ((haspassword,),) = execute(
            ctx,
            instance,
            f"SELECT rolpassword IS NOT NULL FROM pg_authid WHERE rolname = '{rolname}'",
            fetch=True,
        )
        return haspassword  # type: ignore[no-any-return]

    role = manifest.Role(name="nopassword")
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert not has_password(role.name)

    role = manifest.Role(name="password", password="scret")
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert has_password(role.name)
