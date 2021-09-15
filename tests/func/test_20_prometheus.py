from pathlib import Path
from typing import Dict

import pytest
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


def test_configure(ctx, installed, instance, tmp_port_factory):
    prometheus_settings = ctx.settings.prometheus
    stanza = instance.stanza
    configpath = Path(str(prometheus_settings.configpath).format(stanza=stanza))
    assert configpath.exists()

    prometheus_config = config_dict(configpath)
    dsn = prometheus_config["DATA_SOURCE_NAME"]
    assert "user=postgres" in dsn
    assert f"port={instance.port}" in dsn
    port = instance.prometheus.port
    assert prometheus_config["PG_EXPORTER_WEB_LISTEN_ADDRESS"] == f":{port}"

    queriespath = Path(str(prometheus_settings.queriespath).format(stanza=stanza))
    assert queriespath.exists()

    new_port = next(tmp_port_factory)
    with reconfigure_instance(ctx, instance, port=new_port):
        new_prometheus_config = config_dict(configpath)
        dsn = new_prometheus_config["DATA_SOURCE_NAME"]
        assert f"port={new_port}" in dsn


@pytest.fixture
def postgres_exporter(ctx, instance, installed, tmp_port_factory):
    """Setup a postgres_exporter service for 'instance' using another port."""
    port = next(tmp_port_factory)
    name = "123-fo-o"
    role = ctx.settings.postgresql.surole
    dsn = f"dbname=postgres port={instance.port} user={role.name} sslmode=disable"
    host = instance.config().get("unix_socket_directories")
    if host:
        dsn += f" host={host}"
    if role.password:
        dsn += f" password={role.password.get_secret_value()}"
    prometheus.setup(ctx, name, dsn, port)
    prometheus_settings = ctx.settings.prometheus
    configpath = Path(str(prometheus_settings.configpath).format(stanza=name))
    assert configpath.exists()
    queriespath = Path(str(prometheus_settings.queriespath).format(stanza=name))
    assert queriespath.exists()

    yield name, dsn, port

    prometheus.revert_setup(ctx, name, "whatever", port)
    assert not configpath.exists()
    assert not queriespath.exists()


def test_setup(ctx, instance, postgres_exporter):
    name, dsn, port = postgres_exporter
    configpath = Path(str(ctx.settings.prometheus.configpath).format(stanza=name))

    prometheus_config = config_dict(configpath)
    assert f"port={instance.port}" in prometheus_config["DATA_SOURCE_NAME"]
    assert prometheus_config["PG_EXPORTER_WEB_LISTEN_ADDRESS"] == f":{port}"


@retry(reraise=True, wait=wait_fixed(1), stop=stop_after_attempt(3))
def request_metrics(port: int) -> requests.Response:
    return requests.get(f"http://0.0.0.0:{port}/metrics")


def test_start_stop(ctx, installed, instance):
    port = instance.prometheus.port

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, prometheus.systemd_unit(instance))

    with instance_mod.running(ctx, instance, run_hooks=True):
        if ctx.settings.service_manager == "systemd":
            assert systemd.is_active(ctx, prometheus.systemd_unit(instance))
        try:
            r = request_metrics(port)
        except requests.ConnectionError as e:
            raise AssertionError(f"HTTP connection failed: {e}")
        r.raise_for_status()
        assert r.ok
        output = r.text
        assert "pg_up 1" in output.splitlines()

    with instance_mod.stopped(ctx, instance, run_hooks=True):
        if ctx.settings.service_manager == "systemd":
            assert not systemd.is_active(ctx, prometheus.systemd_unit(instance))
        with pytest.raises(requests.ConnectionError):
            request_metrics(port)


def test_start_stop_nonlocal(ctx, instance, postgres_exporter):
    name, dsn, port = postgres_exporter

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, prometheus.systemd_unit(name))

    with instance_mod.running(ctx, instance, run_hooks=False):
        prometheus.start(ctx, name)
        try:
            if ctx.settings.service_manager == "systemd":
                assert systemd.is_active(ctx, prometheus.systemd_unit(name))
            try:
                r = request_metrics(port)
            except requests.ConnectionError as e:
                raise AssertionError(f"HTTP connection failed: {e}")
            r.raise_for_status()
            assert r.ok
            output = r.text
            assert "pg_up 1" in output.splitlines()
        finally:
            prometheus.stop(ctx, name)

        if ctx.settings.service_manager == "systemd":
            assert not systemd.is_active(ctx, prometheus.systemd_unit(name))
        with pytest.raises(requests.ConnectionError):
            request_metrics(port)
