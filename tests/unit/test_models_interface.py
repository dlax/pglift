import pytest

from pglift import pm
from pglift import prometheus as prometheus_mod
from pglift import types
from pglift.models import interface


def test_instance_composite_service(
    plugin_manager: pm.PluginManager, pg_version: str, prometheus: bool
) -> None:
    Instance = interface.Instance.composite(plugin_manager)
    m = Instance.parse_obj({"name": "test", "version": pg_version, "prometheus": None})
    if prometheus:
        s = m.service(prometheus_mod.ServiceManifest)
        assert s is None

    m = Instance.parse_obj(
        {"name": "test", "version": pg_version, "prometheus": {"port": 123}}
    )
    if prometheus:
        s = m.service(prometheus_mod.ServiceManifest)
        assert s is not None and s.port == 123

    class MyService(types.ServiceManifest, service_name="notfound"):
        pass

    with pytest.raises(ValueError, match="notfound"):
        m.service(MyService)
