import pytest

from pglift import types
from pglift.ctx import Context
from pglift.models import interface
from pglift.prometheus import models as prometheus_models


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


def test_instance_composite_service(
    ctx: Context, pg_version: str, prometheus: bool
) -> None:
    Instance = interface.Instance.composite(ctx.pm)
    m = Instance.parse_obj({"name": "test", "version": pg_version, "prometheus": None})
    if prometheus:
        s = m.service(prometheus_models.ServiceManifest)
        assert s is None

    m = Instance.parse_obj(
        {"name": "test", "version": pg_version, "prometheus": {"port": 123}}
    )
    if prometheus:
        s = m.service(prometheus_models.ServiceManifest)
        assert s is not None and s.port == 123

    class MyService(types.ServiceManifest, service_name="notfound"):
        pass

    with pytest.raises(ValueError, match="notfound"):
        m.service(MyService)
