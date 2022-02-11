import logging
from pathlib import Path
from typing import Dict, Iterator, Tuple, Type

import pytest
import requests
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from pglift import exceptions
from pglift import instance as instance_mod
from pglift import prometheus, systemd
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.settings import PrometheusSettings

from . import reconfigure_instance


def config_dict(configpath: Path) -> Dict[str, str]:
    config = {}
    for line in configpath.read_text().splitlines():
        key, value = line.split("=", 1)
        config[key] = value.strip()
    return config


def test_configure(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    instance: system.Instance,
    instance_manifest: interface.Instance,
    tmp_port_factory: Iterator[int],
) -> None:
    service = instance.service(prometheus.Service)
    name = instance.qualname
    configpath = Path(str(prometheus_settings.configpath).format(name=name))
    assert configpath.exists()

    prometheus_config = config_dict(configpath)
    dsn = prometheus_config["DATA_SOURCE_NAME"]
    assert "user=postgres" in dsn
    assert f"port={instance.port}" in dsn
    port = service.port
    assert prometheus_config["PG_EXPORTER_WEB_LISTEN_ADDRESS"] == f":{port}"

    queriespath = Path(str(prometheus_settings.queriespath).format(name=name))
    assert queriespath.exists()

    new_port = next(tmp_port_factory)
    with reconfigure_instance(ctx, instance_manifest, port=new_port):
        new_prometheus_config = config_dict(configpath)
        dsn = new_prometheus_config["DATA_SOURCE_NAME"]
        assert f"port={new_port}" in dsn


@pytest.fixture
def postgres_exporter(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    tmp_port_factory: Iterator[int],
) -> Iterator[Tuple[str, str, int]]:
    """Setup a postgres_exporter service for 'instance' using another port."""
    port = next(tmp_port_factory)
    name = "123-fo-o"
    role = instance_manifest.surole(ctx.settings)
    dsn = f"dbname=postgres port={instance.port} user={role.name} sslmode=disable"
    host = instance.config().get("unix_socket_directories")
    if host:
        dsn += f" host={host}"
    password = None
    if role.password:
        password = role.password.get_secret_value()
    prometheus.setup(
        ctx, name, prometheus_settings, dsn=dsn, password=password, port=port
    )
    configpath = Path(str(prometheus_settings.configpath).format(name=name))
    assert configpath.exists()
    queriespath = Path(str(prometheus_settings.queriespath).format(name=name))
    assert queriespath.exists()

    yield name, dsn, port

    prometheus.revert_setup(ctx, name, prometheus_settings)
    assert not configpath.exists()
    assert not queriespath.exists()


def test_setup(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    instance: system.Instance,
    postgres_exporter: Tuple[str, str, int],
) -> None:
    name, dsn, port = postgres_exporter
    configpath = Path(str(prometheus_settings.configpath).format(name=name))

    prometheus_config = config_dict(configpath)
    assert f"port={instance.port}" in prometheus_config["DATA_SOURCE_NAME"]
    assert prometheus_config["PG_EXPORTER_WEB_LISTEN_ADDRESS"] == f":{port}"


@retry(reraise=True, wait=wait_fixed(1), stop=stop_after_attempt(3))
def request_metrics(port: int) -> requests.Response:
    return requests.get(f"http://0.0.0.0:{port}/metrics")


@pytest.mark.usefixtures("prometheus_available")
def test_start_stop(ctx: Context, instance: system.Instance) -> None:
    service = instance.service(prometheus.Service)
    port = service.port

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, prometheus.systemd_unit(instance.qualname))

    with instance_mod.running(ctx, instance, run_hooks=True):
        if ctx.settings.service_manager == "systemd":
            assert systemd.is_active(ctx, prometheus.systemd_unit(instance.qualname))
        try:
            r = request_metrics(port)
        except requests.ConnectionError as e:
            raise AssertionError(f"HTTP connection failed: {e}") from None
        r.raise_for_status()
        assert r.ok
        output = r.text
        assert "pg_up 1" in output.splitlines()

    with instance_mod.stopped(ctx, instance, run_hooks=True):
        if ctx.settings.service_manager == "systemd":
            assert not systemd.is_active(
                ctx, prometheus.systemd_unit(instance.qualname)
            )
        with pytest.raises(requests.ConnectionError):
            request_metrics(port)


def test_start_stop_nonlocal(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    instance: system.Instance,
    postgres_exporter: Tuple[str, str, int],
) -> None:
    name, dsn, port = postgres_exporter

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, prometheus.systemd_unit(name))

    with instance_mod.running(ctx, instance, run_hooks=False):
        prometheus.start(ctx, name, prometheus_settings)
        try:
            if ctx.settings.service_manager == "systemd":
                assert systemd.is_active(ctx, prometheus.systemd_unit(name))
            try:
                r = request_metrics(port)
            except requests.ConnectionError as e:
                raise AssertionError(f"HTTP connection failed: {e}") from None
            r.raise_for_status()
            assert r.ok
            output = r.text
            assert "pg_up 1" in output.splitlines()
        finally:
            prometheus.stop(ctx, name, prometheus_settings)

        if ctx.settings.service_manager == "systemd":
            assert not systemd.is_active(ctx, prometheus.systemd_unit(name))
        with pytest.raises(requests.ConnectionError):
            request_metrics(port)


def test_apply(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    tmp_port_factory: Iterator[int],
) -> None:
    port = next(tmp_port_factory)
    m = prometheus.PostgresExporter(name="test", dsn="dbname=test", port=port)
    prometheus.apply(ctx, m, prometheus_settings)

    configpath = Path(str(prometheus_settings.configpath).format(name="test"))
    assert configpath.exists()
    queriespath = Path(str(prometheus_settings.queriespath).format(name="test"))
    assert queriespath.exists()

    prometheus_config = config_dict(configpath)
    assert prometheus_config["PG_EXPORTER_WEB_LISTEN_ADDRESS"] == f":{port}"

    port1 = next(tmp_port_factory)
    prometheus.apply(ctx, m.copy(update={"port": port1}), prometheus_settings)
    prometheus_config = config_dict(configpath)
    assert prometheus_config["PG_EXPORTER_WEB_LISTEN_ADDRESS"] == f":{port1}"

    prometheus.apply(
        ctx,
        prometheus.PostgresExporter(name="test", dsn="", port=port, state="absent"),
        prometheus_settings,
    )
    assert not configpath.exists()
    assert not queriespath.exists()


def test_drop_exists(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    tmp_port_factory: Iterator[int],
    caplog: pytest.LogCaptureFixture,
) -> None:
    port = next(tmp_port_factory)
    prometheus.setup(ctx, "dropme", prometheus_settings, port=port)
    assert prometheus.port("dropme", prometheus_settings) == port
    assert prometheus.exists(ctx, "dropme")
    prometheus.drop(ctx, "dropme")
    assert not prometheus.exists(ctx, "dropme")
    with pytest.raises(exceptions.FileNotFoundError, match="postgres_exporter config"):
        prometheus.port("dropme", prometheus_settings)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="pglift"):
        prometheus.drop(ctx, "dropme")
    assert caplog.records[0].message == "no postgres_exporter service 'dropme' found"


@pytest.fixture
def instance_no_prometheus(
    ctx: Context,
    composite_instance_model: Type[interface.Instance],
    tmp_port_factory: Iterator[int],
) -> Iterator[system.Instance]:
    if ctx.settings.prometheus is None:
        pytest.skip("prometheus not enabled")
    im = composite_instance_model.parse_obj(
        {
            "name": "noprom",
            "port": next(tmp_port_factory),
            "prometheus": None,
        }
    )
    r = instance_mod.apply(ctx, im)
    assert r is not None
    instance = r[0]
    yield instance
    instance_mod.drop(ctx, instance)


def test_instance_no_prometheus(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    instance_no_prometheus: system.Instance,
) -> None:
    """Make sure we can create an instance without postgres_exporter and have
    it running and restarted.
    """
    assert not prometheus.enabled(instance_no_prometheus.name, prometheus_settings)
    assert (
        instance_mod.status(ctx, instance_no_prometheus) == instance_mod.Status.running
    )
    instance_mod.restart(ctx, instance_no_prometheus)
    assert (
        instance_mod.status(ctx, instance_no_prometheus) == instance_mod.Status.running
    )
