from pathlib import Path
from typing import Any

from pgtoolkit.conf import Configuration

from . import hookimpl, systemd
from .ctx import BaseContext
from .models.system import BaseInstance, Instance, InstanceSpec
from .settings import PrometheusSettings
from .task import task


def _configpath(instance: BaseInstance, settings: PrometheusSettings) -> Path:
    return Path(str(settings.configpath).format(instance=instance))


def _queriespath(instance: BaseInstance, settings: PrometheusSettings) -> Path:
    return Path(str(settings.queriespath).format(instance=instance))


def systemd_unit(instance: BaseInstance) -> str:
    return f"postgres_exporter@{instance.version}-{instance.name}.service"


def port(ctx: BaseContext, instance: BaseInstance) -> int:
    """Return postgres_exporter port read from configuration file.

    :raises LookupError: if port could not be read from configuration file.
    :raises FileNotFoundError: if configuration file is not found.
    """
    configpath = _configpath(instance, ctx.settings.prometheus)
    varname = "PG_EXPORTER_WEB_LISTEN_ADDRESS"
    with configpath.open() as f:
        for line in f:
            if line.startswith(varname):
                break
        else:
            raise LookupError(f"{varname} not found in {configpath}")
    try:
        value = line.split("=", 1)[1].split(":", 1)[1]
    except (IndexError, ValueError):
        raise LookupError(f"malformatted {varname} parameter in {configpath}")
    return int(value.strip())


@task
def setup(
    ctx: BaseContext, instance: InstanceSpec, instance_config: Configuration
) -> None:
    """Setup postgres_exporter for Prometheus"""
    settings = ctx.settings.prometheus
    configpath = _configpath(instance, settings)
    role = ctx.settings.postgresql.surole
    configpath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)

    dsn = []
    if "port" in instance_config:
        dsn.append(f"port={instance_config.port}")
    host = instance_config.get("unix_socket_directories")
    if host:
        dsn.append(f"host={host}")
    dsn.append(f"user={role.name}")
    if role.password:
        dsn.append(f"password={role.password.get_secret_value()}")
    if not instance_config.ssl:
        dsn.append("sslmode=disable")
    config = [
        f"DATA_SOURCE_NAME={' '.join(dsn)}",
    ]
    appname = f"postgres_exporter-{instance.version}-{instance.name}"
    opts = " ".join(
        [
            "--log.level=info",
            f"--log.format=logger:syslog?appname={appname}&local=0",
        ]
    )
    queriespath = _queriespath(instance, settings)
    config.extend(
        [
            f"PG_EXPORTER_WEB_LISTEN_ADDRESS=:{instance.prometheus.port}",
            "PG_EXPORTER_AUTO_DISCOVER_DATABASES=true",
            f"PG_EXPORTER_EXTEND_QUERY_PATH={queriespath}",
            f"POSTGRES_EXPORTER_OPTS='{opts}'",
        ]
    )

    actual_config = []
    if configpath.exists():
        actual_config = configpath.read_text().splitlines()
    if config != actual_config:
        configpath.write_text("\n".join(config))
    configpath.chmod(0o600)

    if not queriespath.exists():
        queriespath.touch()

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(instance))


@setup.revert
def revert_setup(
    ctx: BaseContext, instance: InstanceSpec, instance_config: Configuration
) -> None:
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
def instance_configure(
    ctx: BaseContext, instance: InstanceSpec, config: Configuration, **kwargs: Any
) -> None:
    """Install postgres_exporter for an instance when it gets configured."""
    setup(ctx, instance, config)


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
    revert_setup(ctx, instance.as_spec(), instance.config())
