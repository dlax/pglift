from pathlib import Path

from pglift import instance as instance_mod
from pglift import prometheus, systemd


def test(ctx, installed, instance):
    prometheus_settings = ctx.settings.prometheus
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
        with instance_mod.running(ctx, instance, run_hooks=True):
            assert systemd.is_active(ctx, prometheus.systemd_unit(instance))

    prometheus.revert_setup(ctx, instance)
    assert not configpath.exists()
    assert not queriespath.exists()
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_enabled(ctx, prometheus.systemd_unit(instance))
