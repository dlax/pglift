from pathlib import Path

import pytest

from pglift import prometheus, systemd

from . import instance_running


@pytest.fixture
def ctx(ctx):
    ctx.pm.unregister(prometheus)
    return ctx


def test(ctx, installed, instance):
    prometheus_settings = ctx.settings.prometheus
    prometheus.setup(ctx, instance)
    configpath = Path(str(prometheus_settings.configpath).format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    instance_config = instance.config()
    assert instance_config
    assert f"DATA_SOURCE_URI=localhost:{instance_config.port}" in lines
    queriespath = Path(str(prometheus_settings.queriespath).format(instance=instance))
    assert queriespath.exists()

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, prometheus.systemd_unit(instance))
        try:
            # Temporarily register back prometheus' hooks so that service
            # gets started at instance startup.
            ctx.pm.register(prometheus)
            with instance_running(ctx, instance):
                assert systemd.is_active(ctx, prometheus.systemd_unit(instance))
        finally:
            ctx.pm.unregister(prometheus)

    prometheus.revert_setup(ctx, instance)
    assert not configpath.exists()
    assert not queriespath.exists()
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_enabled(ctx, prometheus.systemd_unit(instance))
