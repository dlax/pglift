from pathlib import Path
from typing import Dict

import requests
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from pglift import instance as instance_mod
from pglift import prometheus, systemd

from . import reconfigure_instance


def config_dict(configpath: Path) -> Dict[str, str]:
    config = {}
    for line in configpath.read_text().splitlines():
        key, value = line.split("=", 1)
        config[key] = value.strip()
    return config


def test(ctx, installed, instance, tmp_port_factory):
    prometheus_settings = ctx.settings.prometheus
    configpath = Path(str(prometheus_settings.configpath).format(instance=instance))
    assert configpath.exists()

    prometheus_config = config_dict(configpath)
    dsn = prometheus_config["DATA_SOURCE_NAME"]
    assert "user=postgres" in dsn
    assert f"port={instance.port}" in dsn
    port = instance.prometheus.port
    assert prometheus_config["PG_EXPORTER_WEB_LISTEN_ADDRESS"] == f":{port}"

    queriespath = Path(str(prometheus_settings.queriespath).format(instance=instance))
    assert queriespath.exists()

    @retry(reraise=True, wait=wait_fixed(1), stop=stop_after_attempt(3))  # type: ignore[no-untyped-call]
    def request_metrics() -> requests.Response:
        return requests.get(f"http://0.0.0.0:{port}/metrics")

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, prometheus.systemd_unit(instance))
        with instance_mod.running(ctx, instance, run_hooks=True):
            assert systemd.is_active(ctx, prometheus.systemd_unit(instance))
            try:
                r = request_metrics()
            except requests.ConnectionError as e:
                raise AssertionError(f"HTTP connection failed: {e}")
            r.raise_for_status()
        assert r.ok
        output = r.text
        assert "pg_up 1" in output.splitlines()

    new_port = next(tmp_port_factory)
    with reconfigure_instance(ctx, instance, port=new_port):
        new_prometheus_config = config_dict(configpath)
        dsn = new_prometheus_config["DATA_SOURCE_NAME"]
        assert f"port={new_port}" in dsn
