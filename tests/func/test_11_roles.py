import datetime
import functools
import pathlib
from typing import Optional, Union

import psycopg2
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


def test_exists(ctx, instance, role_factory):
    assert not roles.exists(ctx, instance, "absent")
    role_factory("present")
    assert roles.exists(ctx, instance, "present")


def test_create(ctx, instance):
    role = interface.Role(name="nopassword")
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert not roles.has_password(ctx, instance, role)

    role = interface.Role(
        name="password",
        password="scret",
        login=True,
        connection_limit=5,
        validity=datetime.datetime(2050, 1, 2, tzinfo=datetime.timezone.utc),
        in_roles=["pg_monitor"],
    )
    assert not roles.exists(ctx, instance, role.name)
    roles.create(ctx, instance, role)
    assert roles.exists(ctx, instance, role.name)
    assert roles.has_password(ctx, instance, role)
    r = execute(ctx, instance, "select 1", role=role)
    assert r[0][0] == 1
    ((valid_until, connection_limit),) = execute(
        ctx,
        instance,
        f"select rolvaliduntil, rolconnlimit from pg_roles where rolname = '{role.name}'",
        role=role,
    )
    assert valid_until == role.validity
    assert connection_limit == role.connection_limit
    r = execute(
        ctx,
        instance,
        """
        SELECT
            r.rolname AS role,
            ARRAY_AGG(m.rolname) AS member_of
        FROM
            pg_auth_members
            JOIN pg_authid m ON pg_auth_members.roleid = m.oid
            JOIN pg_authid r ON pg_auth_members.member = r.oid
        GROUP BY
            r.rolname
        """,
    )
    assert ["password", ["pg_monitor"]] in r

    nologin = interface.Role(name="nologin", password="passwd", login=False)
    roles.create(ctx, instance, nologin)
    with pytest.raises(
        psycopg2.OperationalError, match='role "nologin" is not permitted to log in'
    ):
        execute(ctx, instance, "select 1", role=nologin)


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

    role = interface.Role(name=rolname, state="absent")
    assert roles.exists(ctx, instance, role.name)
    roles.apply(ctx, instance, role)
    assert not roles.exists(ctx, instance, role.name)

    role = interface.Role(name=rolname, password=SecretStr("passw0rd"))
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert not _role_in_pgpass(role)

    role = interface.Role(name=rolname, password=SecretStr("passw0rd"), pgpass=True)
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert _role_in_pgpass(role)

    role = interface.Role(
        name=rolname,
        password=SecretStr("passw0rd_changed"),
        pgpass=True,
        connection_limit=5,
    )
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert _role_in_pgpass(role)
    assert roles.describe(ctx, instance, rolname).connection_limit == 5

    role = interface.Role(name=rolname, pgpass=False)
    roles.apply(ctx, instance, role)
    assert roles.has_password(ctx, instance, role)
    assert not _role_in_pgpass(role)
    assert roles.describe(ctx, instance, rolname).connection_limit is None


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
    assert postgres.login

    role_factory(
        "r1",
        "LOGIN NOINHERIT VALID UNTIL '2051-07-29T00:00+00:00' IN ROLE pg_monitor CONNECTION LIMIT 10",
    )
    r1 = roles.describe(ctx, instance, "r1")
    assert r1.password is None
    assert not r1.inherit
    assert r1.login
    assert r1.connection_limit == 10
    assert r1.in_roles == ["pg_monitor"]
    assert r1.validity == datetime.datetime(2051, 7, 29, tzinfo=datetime.timezone.utc)


def test_alter(ctx, instance, role_factory):
    role = interface.Role(
        name="alter",
        password="scret",
        login=True,
        connection_limit=5,
        validity=datetime.datetime(2050, 1, 2, tzinfo=datetime.timezone.utc),
        in_roles=["pg_read_all_stats", "pg_signal_backend"],
    )
    with pytest.raises(exceptions.RoleNotFound, match="alter"):
        roles.alter(ctx, instance, role)
    role_factory("alter", "IN ROLE pg_read_all_settings, pg_read_all_stats")
    roles.alter(ctx, instance, role)
    described = roles.describe(ctx, instance, "alter").dict()
    described.pop("password").get_secret_value() == "<set>"
    expected = role.dict()
    del expected["password"]
    assert described == expected


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
