import pytest

from pglift import types
from pglift.ctx import Context
from pglift.models import interface
from pglift.prometheus import models as prometheus_models


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
