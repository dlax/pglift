import shlex
from pathlib import Path
from typing import Any, Dict

from pgtoolkit.conf import Configuration

from . import cmd, exceptions, hookimpl, systemd
from .ctx import BaseContext
from .models.system import BaseInstance, Instance, InstanceSpec
from .settings import PrometheusSettings
from .task import task


def _configpath(instance: BaseInstance, settings: PrometheusSettings) -> Path:
    return Path(str(settings.configpath).format(instance=instance))


def _queriespath(instance: BaseInstance, settings: PrometheusSettings) -> Path:
    return Path(str(settings.queriespath).format(instance=instance))


def _pidfile(instance: BaseInstance, settings: PrometheusSettings) -> Path:
    return Path(str(settings.pid_file).format(instance=instance))


def systemd_unit(instance: BaseInstance) -> str:
    return f"postgres_exporter@{instance.version}-{instance.name}.service"


def port(ctx: BaseContext, instance: BaseInstance) -> int:
    """Return postgres_exporter port read from configuration file.

    :raises ~exceptions.ConfigurationError: if port could not be read from
        configuration file.
    :raises ~exceptions.FileNotFoundError: if configuration file is not found.
    """
    configpath = _configpath(instance, ctx.settings.prometheus)
    if not configpath.exists():
        raise exceptions.FileNotFoundError(
            "postgres_exporter configuration file {configpath} not found"
        )
    varname = "PG_EXPORTER_WEB_LISTEN_ADDRESS"
    with configpath.open() as f:
        for line in f:
            if line.startswith(varname):
                break
        else:
            raise exceptions.ConfigurationError(configpath, f"{varname} not found")
    try:
        value = line.split("=", 1)[1].split(":", 1)[1]
    except (IndexError, ValueError):
        raise exceptions.ConfigurationError(
            configpath, f"malformatted {varname} parameter"
        )
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
    log_options = ["--log.level=info"]
    if ctx.settings.service_manager == "systemd":
        # XXX Checking for systemd presence as a naive way to check for syslog
        # availability; this is enough for Docker.
        log_options.append(f"--log.format=logger:syslog?appname={appname}&local=0")
    opts = " ".join(log_options)
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


def start(ctx: BaseContext, instance: Instance) -> None:
    if ctx.settings.service_manager == "systemd":
        systemd.start(ctx, systemd_unit(instance))
    else:
        settings = ctx.settings.prometheus
        configpath = _configpath(instance, settings)
        env: Dict[str, str] = {}
        for line in configpath.read_text().splitlines():
            key, value = line.split("=", 1)
            env[key] = value
        opts = shlex.split(env.pop("POSTGRES_EXPORTER_OPTS")[1:-1])
        pidfile = _pidfile(instance, settings)
        cmd.execute_program(
            [str(settings.execpath)] + opts, pidfile, env=env, logger=ctx
        )


@hookimpl  # type: ignore[misc]
def instance_start(ctx: BaseContext, instance: Instance) -> None:
    """Start postgres_exporter service."""
    start(ctx, instance)


def stop(ctx: BaseContext, instance: Instance) -> None:
    """Stop postgres_exporter service."""
    if ctx.settings.service_manager == "systemd":
        systemd.stop(ctx, systemd_unit(instance))
    else:
        pidfile = _pidfile(instance, ctx.settings.prometheus)
        cmd.terminate_program(pidfile, logger=ctx)


@hookimpl  # type: ignore[misc]
def instance_stop(ctx: BaseContext, instance: Instance) -> None:
    """Stop postgres_exporter service."""
    stop(ctx, instance)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Uninstall postgres_exporter from an instance being dropped."""
    revert_setup(ctx, instance.as_spec(), instance.config())
