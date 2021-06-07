from pathlib import Path

import requests

from pglift import instance as instance_mod
from pglift import prometheus, systemd


def test(ctx, installed, instance):
    prometheus_settings = ctx.settings.prometheus
    configpath = Path(str(prometheus_settings.configpath).format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    instance_config = instance.config()
    assert instance_config

    for line in lines:
        key, value = line.split("=", 1)
        if key == "DATA_SOURCE_NAME":
            dsn = value
            break
    else:
        raise AssertionError("DATA_SOURCE_NAME not found in config")
    assert "user=postgres" in dsn
    assert f"port={instance_config.port}" in dsn

    queriespath = Path(str(prometheus_settings.queriespath).format(instance=instance))
    assert queriespath.exists()

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, prometheus.systemd_unit(instance))
        with instance_mod.running(ctx, instance, run_hooks=True):
            assert systemd.is_active(ctx, prometheus.systemd_unit(instance))
            try:
                r = requests.get("http://0.0.0.0:9187/metrics")
            except requests.ConnectionError as e:
                raise AssertionError(f"HTTP connection failed: {e}")
            r.raise_for_status()
        assert r.ok
        output = r.text
        assert "pg_up 1" in output.splitlines()
