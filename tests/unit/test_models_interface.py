import socket
from pathlib import Path

import port_for
import pydantic
import pytest

from pglift import types
from pglift.ctx import Context
from pglift.models import interface
from pglift.prometheus import models as prometheus_models
from pglift.settings import Settings


def test_validate_ports() -> None:
    class S(pydantic.BaseModel):
        name: str
        port: types.Port

    class M(pydantic.BaseModel):
        p: types.Port
        s: S

    p1 = port_for.select_random()
    p2 = port_for.select_random()
    m = M.parse_obj({"p": p1, "s": {"name": "x", "port": p2}})
    interface.validate_ports(m)

    with socket.socket() as s1, socket.socket() as s2:
        s1.bind(("", p1))
        s2.bind(("", p2))
        with pytest.raises(pydantic.ValidationError) as cm:
            interface.validate_ports(m)
    assert f"{p1} already in use" in str(cm)
    assert f"{p2} already in use" in str(cm)


def test_instance__auth(
    settings: Settings, instance_manifest: interface.Instance
) -> None:
    assert instance_manifest._auth(settings.postgresql.auth) == interface.Instance.Auth(
        local="peer", host="password"
    )


def test_instance_pg_hba(
    ctx: Context,
    instance_manifest: interface.Instance,
    datadir: Path,
    write_changes: bool,
) -> None:
    actual = instance_manifest.pg_hba(ctx)
    fpath = datadir / "pg_hba.conf"
    if write_changes:
        fpath.write_text(actual)
    expected = fpath.read_text()
    assert actual == expected


def test_instance_pg_ident(
    ctx: Context,
    instance_manifest: interface.Instance,
    datadir: Path,
    write_changes: bool,
) -> None:
    actual = instance_manifest.pg_ident(ctx)
    fpath = datadir / "pg_ident.conf"
    if write_changes:
        fpath.write_text(actual)
    expected = fpath.read_text()
    assert actual == expected


def test_privileges_sorted() -> None:
    p = interface.Privilege(
        database="postgres",
        schema="main",
        object_type="table",
        object_name="foo",
        role="postgres",
        privileges=["select", "delete", "update"],
        column_privileges={"postgres": ["update", "delete", "reference"]},
    )
    assert p.dict() == {
        "column_privileges": {"postgres": ["delete", "reference", "update"]},
        "database": "postgres",
        "object_name": "foo",
        "object_type": "table",
        "privileges": ["delete", "select", "update"],
        "role": "postgres",
        "schema_": "main",
    }


def test_instance_composite_service(ctx: Context, pg_version: str) -> None:
    Instance = interface.Instance.composite(ctx.pm)
    m = Instance.parse_obj({"name": "test", "version": pg_version, "prometheus": None})
    s = m.service(prometheus_models.ServiceManifest)
    assert s is None

    m = Instance.parse_obj(
        {"name": "test", "version": pg_version, "prometheus": {"port": 123}}
    )
    s = m.service(prometheus_models.ServiceManifest)
    assert s is not None and s.port == 123

    class MyService(types.ServiceManifest, service_name="notfound"):
        pass

    with pytest.raises(ValueError, match="notfound"):
        m.service(MyService)
