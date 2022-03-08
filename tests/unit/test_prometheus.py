import pathlib

import pydantic
import pytest

from pglift import exceptions
from pglift.ctx import Context
from pglift.models.system import Instance
from pglift.prometheus import impl as prometheus
from pglift.prometheus import (
    install_systemd_unit_template,
    models,
    uninstall_systemd_unit_template,
)
from pglift.settings import PrometheusSettings, Settings


@pytest.fixture
def prometheus_settings(
    need_prometheus: None, settings: Settings
) -> PrometheusSettings:
    assert settings.prometheus is not None
    return settings.prometheus


@pytest.mark.usefixtures("need_prometheus")
def test_systemd_unit(pg_version: str, instance: Instance) -> None:
    assert (
        prometheus.systemd_unit(instance.qualname)
        == f"pglift-postgres_exporter@{pg_version}-test.service"
    )


@pytest.mark.usefixtures("need_prometheus")
def test_install_systemd_unit_template(ctx: Context) -> None:
    install_systemd_unit_template(ctx)
    unit = ctx.settings.systemd.unit_path / "pglift-postgres_exporter@.service"
    assert unit.exists()
    lines = unit.read_text().splitlines()
    assert (
        f"EnvironmentFile=-{ctx.settings.prefix}/etc/prometheus/postgres_exporter-%i.conf"
        in lines
    )
    assert (
        "ExecStart=/usr/bin/prometheus-postgres-exporter $POSTGRES_EXPORTER_OPTS"
        in lines
    )
    uninstall_systemd_unit_template(ctx)
    assert not unit.exists()


def test_port(prometheus_settings: PrometheusSettings, instance: Instance) -> None:
    try:
        prometheus_service = instance.service(models.Service)
    except ValueError:
        prometheus_service = None
    if prometheus_service:
        port = prometheus.port(instance.qualname, prometheus_settings)
        assert port == 9817
    else:
        with pytest.raises(exceptions.FileNotFoundError):
            prometheus.port(instance.qualname, prometheus_settings)

    configpath = pathlib.Path(
        str(prometheus_settings.configpath).format(name=instance.qualname)
    )
    original_content = None
    if prometheus_service:
        original_content = configpath.read_text()
    else:
        configpath.parent.mkdir(parents=True)  # exists not ok
    try:
        configpath.write_text("\nempty\n")
        with pytest.raises(
            LookupError, match="PG_EXPORTER_WEB_LISTEN_ADDRESS not found"
        ):
            prometheus.port(instance.qualname, prometheus_settings)

        configpath.write_text("\nPG_EXPORTER_WEB_LISTEN_ADDRESS=42\n")
        with pytest.raises(
            LookupError, match="malformatted PG_EXPORTER_WEB_LISTEN_ADDRESS"
        ):
            prometheus.port(instance.qualname, prometheus_settings)
    finally:
        if original_content is not None:
            configpath.write_text(original_content)


def test_password(prometheus_settings: PrometheusSettings, instance: Instance) -> None:
    try:
        prometheus_service = instance.service(models.Service)
    except ValueError:
        prometheus_service = None
    if prometheus_service:
        password = prometheus.password(instance.qualname, prometheus_settings)
        assert password == "truite"
    else:
        with pytest.raises(exceptions.FileNotFoundError):
            prometheus.password(instance.qualname, prometheus_settings)

    configpath = pathlib.Path(
        str(prometheus_settings.configpath).format(name=instance.qualname)
    )
    original_content = None
    if prometheus_service:
        original_content = configpath.read_text()
    else:
        configpath.parent.mkdir(parents=True)  # exists not ok
    try:
        configpath.write_text("\nempty\n")
        with pytest.raises(LookupError, match="DATA_SOURCE_NAME not found"):
            prometheus.password(instance.qualname, prometheus_settings)

        configpath.write_text("\nDATA_SOURCE_NAME=foo=bar\n")
        with pytest.raises(LookupError, match="malformatted DATA_SOURCE_NAME"):
            prometheus.password(instance.qualname, prometheus_settings)
    finally:
        if original_content is not None:
            configpath.write_text(original_content)


def test_postgresexporter() -> None:
    m = models.PostgresExporter(name="12-x", dsn="dbname=postgres", port=9876)
    assert m.dsn == "dbname=postgres"
    with pytest.raises(pydantic.ValidationError):
        models.PostgresExporter(dsn="x=y", port=9876)


def test_apply(
    ctx: Context, instance: Instance, prometheus_settings: PrometheusSettings
) -> None:
    m = models.PostgresExporter(name=instance.qualname, dsn="", port=123)
    with pytest.raises(exceptions.InstanceStateError, match="exists locally"):
        prometheus.apply(ctx, m, prometheus_settings)
