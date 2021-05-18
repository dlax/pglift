import textwrap
from pathlib import Path

from . import hookimpl, systemd
from .ctx import BaseContext
from .model import Instance
from .settings import PrometheusSettings
from .task import task


def _configpath(instance: Instance, settings: PrometheusSettings) -> Path:
    return Path(str(settings.configpath).format(instance=instance))


def _queriespath(instance: Instance, settings: PrometheusSettings) -> Path:
    return Path(str(settings.queriespath).format(instance=instance))


def systemd_unit(instance: Instance) -> str:
    """Return systemd unit service name for 'instance'.

    >>> instance = Instance("test", "13")
    >>> systemd_unit(instance)
    'postgres_exporter@13-test.service'
    """
    return f"postgres_exporter@{instance.version}-{instance.name}.service"


@task
def setup(ctx: BaseContext, instance: Instance) -> None:
    """Setup postgres_exporter for Prometheus"""
    settings = ctx.settings.prometheus
    configpath = _configpath(instance, settings)
    content = """
    DATA_SOURCE_URI={dsn}
    DATA_SOURCE_USER={role}
    PG_EXPORTER_WEB_LISTEN_ADDRESS=:{port}
    PG_EXPORTER_AUTO_DISCOVER_DATABASES=true
    PG_EXPORTER_EXTEND_QUERY_PATH={queriespath}
    POSTGRES_EXPORTER_OPTS='--log.level=info --log.format=logger:syslog?appname=postgres_exporter-{instance.version}-{instance.name}&local=0'
    """
    configpath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)
    instance_config = instance.config()
    assert instance_config

    try:
        dsn = f"localhost:{instance_config.port}"
    except AttributeError:
        dsn = "localhost"
    queriespath = _queriespath(instance, settings)
    config = {
        "instance": instance,
        "dsn": dsn,
        "role": ctx.settings.postgresql.surole,
        "port": ctx.settings.prometheus.port,
        "queriespath": queriespath,
    }

    if not configpath.exists():
        with configpath.open("w") as configfile:
            configfile.write(textwrap.dedent(content.format(**config)))
        configpath.chmod(0o600)

    if not queriespath.exists():
        queriespath.touch()

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(instance))


@setup.revert
def revert_setup(ctx: BaseContext, instance: Instance) -> None:
    """Un-setup postgres_exporter for Prometheus"""
    if ctx.settings.service_manager == "systemd":
        unit = systemd_unit(instance)
        systemd.disable(ctx, unit, now=True)

    settings = ctx.settings.prometheus
    configpath = _configpath(instance, settings)

    if configpath.exists():
        configpath.unlink()

    queriespath = _queriespath(instance, settings)
    if queriespath.exists():
        queriespath.unlink()


@hookimpl  # type: ignore[misc]
def instance_configure(ctx: BaseContext, instance: Instance) -> None:
    """Install postgres_exporter for an instance when it gets configured."""
    setup(ctx, instance)


@hookimpl  # type: ignore[misc]
def instance_start(ctx: BaseContext, instance: Instance) -> None:
    """Start postgres_exporter service."""
    if ctx.settings.service_manager == "systemd":
        systemd.start(ctx, systemd_unit(instance))


@hookimpl  # type: ignore[misc]
def instance_stop(ctx: BaseContext, instance: Instance) -> None:
    """Stop postgres_exporter service."""
    if ctx.settings.service_manager == "systemd":
        systemd.stop(ctx, systemd_unit(instance))


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Uninstall postgres_exporter from an instance being dropped."""
    revert_setup(ctx, instance)
