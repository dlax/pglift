import shlex
from pathlib import Path
from typing import Any, Dict, Optional

from pgtoolkit.conf import Configuration

from . import cmd, exceptions, hookimpl
from . import prometheus_default_port as default_port
from . import systemd
from .ctx import BaseContext
from .models import interface
from .models.system import Instance, InstanceSpec, PostgreSQLInstance
from .settings import PrometheusSettings
from .task import task


def enabled(ctx: BaseContext) -> bool:
    return ctx.settings.prometheus.execpath.exists()


def _configpath(name: str, settings: PrometheusSettings) -> Path:
    return Path(str(settings.configpath).format(name=name))


def _queriespath(name: str, settings: PrometheusSettings) -> Path:
    return Path(str(settings.queriespath).format(name=name))


def _pidfile(name: str, settings: PrometheusSettings) -> Path:
    return Path(str(settings.pid_file).format(name=name))


def systemd_unit(name: str) -> str:
    return f"postgres_exporter@{name}.service"


def port(ctx: BaseContext, name: str) -> int:
    """Return postgres_exporter port read from configuration file.

    :param name: the name for the service.

    :raises ~exceptions.ConfigurationError: if port could not be read from
        configuration file.
    :raises ~exceptions.FileNotFoundError: if configuration file is not found.
    """
    configpath = _configpath(name, ctx.settings.prometheus)
    if not configpath.exists():
        raise exceptions.FileNotFoundError(
            f"postgres_exporter configuration file {configpath} not found"
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
    ctx: BaseContext,
    name: str,
    *,
    dsn: str = "",
    password: Optional[str] = None,
    port: int = default_port,
) -> None:
    """Set up a Prometheus postgres_exporter service for an instance.

    :param name: a (locally unique) name for the service.
    :param dsn: connection info string to target instance.
    :param password: connection password.
    :param port: TCP port for the web interface and telemetry of postgres_exporter.
    """
    settings = ctx.settings.prometheus
    if password is not None:
        dsn += f" password={password}"
    config = [f"DATA_SOURCE_NAME={dsn.strip()}"]
    appname = f"postgres_exporter-{name}"
    log_options = ["--log.level=info"]
    if ctx.settings.service_manager == "systemd":
        # XXX Checking for systemd presence as a naive way to check for syslog
        # availability; this is enough for Docker.
        log_options.append(f"--log.format=logger:syslog?appname={appname}&local=0")
    opts = " ".join(log_options)
    queriespath = _queriespath(name, settings)
    config.extend(
        [
            f"PG_EXPORTER_WEB_LISTEN_ADDRESS=:{port}",
            "PG_EXPORTER_AUTO_DISCOVER_DATABASES=true",
            f"PG_EXPORTER_EXTEND_QUERY_PATH={queriespath}",
            f"POSTGRES_EXPORTER_OPTS='{opts}'",
        ]
    )

    configpath = _configpath(name, settings)
    configpath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)
    actual_config = []
    if configpath.exists():
        actual_config = configpath.read_text().splitlines()
    if config != actual_config:
        configpath.write_text("\n".join(config))
    configpath.chmod(0o600)

    if not queriespath.exists():
        queriespath.touch()

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(name))


@setup.revert
def revert_setup(
    ctx: BaseContext,
    name: str,
    *,
    dsn: str = "",
    password: Optional[str] = None,
    port: int = default_port,
) -> None:
    if ctx.settings.service_manager == "systemd":
        unit = systemd_unit(name)
        systemd.disable(ctx, unit, now=True)

    settings = ctx.settings.prometheus
    configpath = _configpath(name, settings)

    if configpath.exists():
        configpath.unlink()

    queriespath = _queriespath(name, settings)
    if queriespath.exists():
        queriespath.unlink()


@task
def exists(ctx: BaseContext, name: str) -> bool:
    """Return True if a postgres_exporter with `name` exists locally."""
    try:
        port(ctx, name)
    except exceptions.FileNotFoundError:
        return False
    return True


def apply(ctx: BaseContext, manifest: interface.PostgresExporter) -> None:
    """Apply state described by specified manifest as a postgres_exporter
    service for a non-local instance.

    :raises exceptions.InstanceStateError: if the target instance exists on system.
    """
    try:
        PostgreSQLInstance.from_stanza(ctx, manifest.name)
    except (ValueError, exceptions.InstanceNotFound):
        pass
    else:
        raise exceptions.InstanceStateError(
            f"instance '{manifest.name}' exists locally"
        )

    if manifest.state == interface.PostgresExporter.State.absent:
        drop(ctx, manifest.name)
    else:
        # TODO: detect if setup() actually need to be called by comparing
        # manifest with system state.
        password = None
        if manifest.password:
            password = manifest.password.get_secret_value()
        setup(
            ctx, manifest.name, dsn=manifest.dsn, password=password, port=manifest.port
        )
        if manifest.state == interface.PostgresExporter.State.started:
            start(ctx, manifest.name)
        elif manifest.state == interface.PostgresExporter.State.stopped:
            stop(ctx, manifest.name)


@task
def drop(ctx: BaseContext, name: str) -> None:
    """Remove a postgres_exporter service."""
    if not exists(ctx, name):
        ctx.warning("no postgres_exporter service '%s' found", name)
        return

    stop(ctx, name)
    revert_setup(ctx, name)


@task
def setup_local(
    ctx: BaseContext, instance: InstanceSpec, instance_config: Configuration
) -> None:
    """Setup Prometheus postgres_exporter for a local instance."""
    role = ctx.settings.postgresql.surole
    dsn = []
    if "port" in instance_config:
        dsn.append(f"port={instance_config.port}")
    host = instance_config.get("unix_socket_directories")
    if host:
        dsn.append(f"host={host}")
    dsn.append(f"user={role.name}")
    if not instance_config.ssl:
        dsn.append("sslmode=disable")
    password = None
    if role.password:
        password = role.password.get_secret_value()
    setup(
        ctx,
        instance.qualname,
        dsn=" ".join(dsn),
        password=password,
        port=instance.prometheus.port,
    )


@setup_local.revert
def revert_setup_local(
    ctx: BaseContext, instance: InstanceSpec, instance_config: Configuration
) -> None:
    """Un-setup Prometheus postgres_exporter for a local instance."""
    revert_setup(ctx, instance.qualname)


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: BaseContext, instance: InstanceSpec, config: Configuration, **kwargs: Any
) -> None:
    """Install postgres_exporter for an instance when it gets configured."""
    if not enabled(ctx):
        ctx.warning(
            "Prometheus postgres_exporter not available, skipping monitoring configuration"
        )
        return
    setup_local(ctx, instance, config)


def start(ctx: BaseContext, name: str, *, foreground: bool = False) -> None:
    """Start postgres_exporter for `instance`.

    :param name: the name for the service.
    :param foreground: start the program in foreground, replacing the current process.
    :raises ValueError: if 'foreground' does not apply with site configuration.
    """
    if ctx.settings.service_manager == "systemd":
        if foreground:
            raise ValueError("'foreground' parameter does not apply with systemd")
        systemd.start(ctx, systemd_unit(name))
    else:
        settings = ctx.settings.prometheus
        configpath = _configpath(name, settings)
        env: Dict[str, str] = {}
        for line in configpath.read_text().splitlines():
            key, value = line.split("=", 1)
            env[key] = value
        opts = shlex.split(env.pop("POSTGRES_EXPORTER_OPTS")[1:-1])
        args = [str(settings.execpath)] + opts
        if foreground:
            cmd.execute_program(args, env=env, logger=ctx)
        else:
            pidfile = _pidfile(name, settings)
            if cmd.status_program(pidfile) == cmd.Status.running:
                ctx.debug("postgres_exporter '%s' is already running", name)
                return
            cmd.start_program(args, pidfile, env=env, logger=ctx)


@hookimpl  # type: ignore[misc]
def instance_start(ctx: BaseContext, instance: Instance) -> None:
    """Start postgres_exporter service."""
    if not enabled(ctx):
        return
    start(ctx, instance.qualname)


def stop(ctx: BaseContext, name: str) -> None:
    """Stop postgres_exporter service."""
    if ctx.settings.service_manager == "systemd":
        systemd.stop(ctx, systemd_unit(name))
    else:
        pidfile = _pidfile(name, ctx.settings.prometheus)
        if cmd.status_program(pidfile) == cmd.Status.not_running:
            ctx.debug("postgres_exporter '%s' is already stopped", name)
            return
        cmd.terminate_program(pidfile, logger=ctx)


@hookimpl  # type: ignore[misc]
def instance_stop(ctx: BaseContext, instance: Instance) -> None:
    """Stop postgres_exporter service."""
    if not enabled(ctx):
        return
    stop(ctx, instance.qualname)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Uninstall postgres_exporter from an instance being dropped."""
    revert_setup_local(ctx, instance.as_spec(), instance.config())
