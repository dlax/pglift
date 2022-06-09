import logging
import shlex
import urllib.parse
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

import psycopg
import psycopg.conninfo

from .. import cmd, exceptions, instances, roles, systemd
from ..models import interface, system
from ..task import task
from .models import PostgresExporter, ServiceManifest, default_port

if TYPE_CHECKING:
    from pgtoolkit.conf import Configuration

    from ..ctx import BaseContext
    from ..settings import PrometheusSettings

logger = logging.getLogger(__name__)


def available(ctx: "BaseContext") -> Optional["PrometheusSettings"]:
    return ctx.settings.prometheus


def enabled(qualname: str, settings: "PrometheusSettings") -> bool:
    return _configpath(qualname, settings).exists()


def _configpath(qualname: str, settings: "PrometheusSettings") -> Path:
    return Path(str(settings.configpath).format(name=qualname))


def _queriespath(qualname: str, settings: "PrometheusSettings") -> Path:
    return Path(str(settings.queriespath).format(name=qualname))


def _pidfile(qualname: str, settings: "PrometheusSettings") -> Path:
    return Path(str(settings.pid_file).format(name=qualname))


def systemd_unit(qualname: str) -> str:
    return f"pglift-postgres_exporter@{qualname}.service"


def config_var(configpath: Path, varname: str) -> str:
    """Return postgres_exporter configuration file line for given varname.

    :param configpath: the path to the configuration file.
    :param varname: the name of the variable to search for.

    :raises ~exceptions.ConfigurationError: if varname could not be read from
        configuration file.
    :raises ~exceptions.FileNotFoundError: if configuration file is not found.
    """
    if not configpath.exists():
        raise exceptions.FileNotFoundError(
            f"postgres_exporter configuration file {configpath} not found"
        )
    with configpath.open() as f:
        for line in f:
            if line.startswith(varname):
                break
        else:
            raise exceptions.ConfigurationError(configpath, f"{varname} not found")
    return line


def port(name: str, settings: "PrometheusSettings") -> int:
    """Return postgres_exporter port read from configuration file.

    :param name: the name for the service.

    :raises ~exceptions.ConfigurationError: if port could not be read from
        configuration file.
    :raises ~exceptions.FileNotFoundError: if configuration file is not found.
    """
    configpath = _configpath(name, settings)
    varname = "PG_EXPORTER_WEB_LISTEN_ADDRESS"
    line = config_var(configpath, varname)
    try:
        value = line.split("=", 1)[1].split(":", 1)[1]
    except (IndexError, ValueError):
        raise exceptions.ConfigurationError(
            configpath, f"malformatted {varname} parameter"
        )
    return int(value.strip())


def password(name: str, settings: "PrometheusSettings") -> Optional[str]:
    """Return postgres_exporter dsn password read from configuration file.

    :param name: the name for the service.

    :raises ~exceptions.ConfigurationError: if password could not be read from
        configuration file.
    :raises ~exceptions.FileNotFoundError: if configuration file is not found.
    """
    configpath = _configpath(name, settings)
    varname = "DATA_SOURCE_NAME"
    line = config_var(configpath, varname)
    try:
        conninfo = psycopg.conninfo.conninfo_to_dict(line.split("=", 1)[1])
        value: Optional[str] = conninfo.get("password")
    except (IndexError, ValueError, psycopg.ProgrammingError):
        raise exceptions.ConfigurationError(
            configpath, f"malformatted {varname} parameter"
        )
    return value


def make_uri(
    *,
    user: str = "",
    password: str = "",
    port: str = "5432",
    dbname: str = "",
    **kw: str,
) -> str:
    """Return a libpq compatible uri for the given dsn object

    Note: key=value form DSN doesn't work with a unix socket host.
    Also for socket hosts, the host must be given in the uri params
    (after '?').

    >>> make_uri(**{'host': '/socket/path', 'dbname': 'somedb'})
    'postgresql://:5432/somedb?host=%2Fsocket%2Fpath'
    >>> make_uri(**{'host': '/socket/path'})
    'postgresql://:5432?host=%2Fsocket%2Fpath'
    >>> make_uri(**{'host': '/socket/path', 'user': 'someone', 'dbname': 'somedb', 'connect_timeout': '10', 'password': 'secret'})
    'postgresql://someone:secret@:5432/somedb?host=%2Fsocket%2Fpath&connect_timeout=10'
    """
    userspec = user
    userspec += f":{password}" if password else ""
    userspec += "@" if userspec else ""
    netloc = f"{userspec}:{port}"
    query = urllib.parse.urlencode(kw)
    return urllib.parse.urlunsplit(("postgresql", netloc, dbname, query, None))


@task("setting up Prometheus postgres_exporter service")
def setup(
    ctx: "BaseContext",
    name: str,
    settings: "PrometheusSettings",
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
    uri = make_uri(**psycopg.conninfo.conninfo_to_dict(dsn, password=password))
    config = [f"DATA_SOURCE_NAME={uri}"]
    log_options = ["--log.level=info"]
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
        queriespath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)
        queriespath.touch()

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(name))


@setup.revert("deconfiguring postgres_exporter service")
def revert_setup(
    ctx: "BaseContext",
    name: str,
    settings: "PrometheusSettings",
    *,
    dsn: str = "",
    password: Optional[str] = None,
    port: int = default_port,
) -> None:
    if ctx.settings.service_manager == "systemd":
        unit = systemd_unit(name)
        systemd.disable(ctx, unit, now=True)

    configpath = _configpath(name, settings)

    if configpath.exists():
        configpath.unlink()

    queriespath = _queriespath(name, settings)
    if queriespath.exists():
        queriespath.unlink()


@task("checking existence of postgres_exporter service locally")
def exists(ctx: "BaseContext", name: str) -> bool:
    """Return True if a postgres_exporter with `name` exists locally."""
    settings = available(ctx)
    if not settings:
        return False
    try:
        port(name, settings)
    except exceptions.FileNotFoundError:
        return False
    return True


@task("starting postgres_exporter service")
def start(
    ctx: "BaseContext",
    name: str,
    settings: "PrometheusSettings",
    *,
    foreground: bool = False,
) -> None:
    """Start postgres_exporter for `instance`.

    :param name: the name for the service.
    :param foreground: start the program in foreground, replacing the current process.
    :raises ValueError: if 'foreground' does not apply with site configuration.
    :raises ~exceptions.InstanceNotFound: if 'name' service does not exist.
    """
    if not enabled(name, settings):
        raise exceptions.InstanceNotFound(name)
    if ctx.settings.service_manager == "systemd":
        if foreground:
            raise ValueError("'foreground' parameter does not apply with systemd")
        systemd.start(ctx, systemd_unit(name))
    else:
        configpath = _configpath(name, settings)
        env: Dict[str, str] = {}
        for line in configpath.read_text().splitlines():
            key, value = line.split("=", 1)
            env[key] = value
        opts = shlex.split(env.pop("POSTGRES_EXPORTER_OPTS")[1:-1])
        args = [str(settings.execpath)] + opts
        if foreground:
            cmd.execute_program(args, env=env, logger=logger)
        else:
            pidfile = _pidfile(name, settings)
            if cmd.status_program(pidfile) == cmd.Status.running:
                logger.debug("postgres_exporter '%s' is already running", name)
                return
            cmd.start_program(args, pidfile, env=env, logger=logger)


@task("stopping postgres_exporter service")
def stop(ctx: "BaseContext", name: str, settings: "PrometheusSettings") -> None:
    """Stop postgres_exporter service.

    :raises ~exceptions.InstanceNotFound: if 'name' service does not exist.
    """
    if not enabled(name, settings):
        raise exceptions.InstanceNotFound(name)
    if ctx.settings.service_manager == "systemd":
        systemd.stop(ctx, systemd_unit(name))
    else:
        pidfile = _pidfile(name, settings)
        if cmd.status_program(pidfile) == cmd.Status.not_running:
            logger.debug("postgres_exporter '%s' is already stopped", name)
            return
        cmd.terminate_program(pidfile, logger=logger)


def apply(
    ctx: "BaseContext",
    postgres_exporter: PostgresExporter,
    settings: "PrometheusSettings",
) -> None:
    """Apply state described by specified interface model as a postgres_exporter
    service for a non-local instance.

    :raises exceptions.InstanceStateError: if the target instance exists on system.
    """
    try:
        system.PostgreSQLInstance.from_stanza(ctx, postgres_exporter.name)
    except (ValueError, exceptions.InstanceNotFound):
        pass
    else:
        raise exceptions.InstanceStateError(
            f"instance '{postgres_exporter.name}' exists locally"
        )

    if postgres_exporter.state == PostgresExporter.State.absent:
        drop(ctx, postgres_exporter.name)
    else:
        # TODO: detect if setup() actually need to be called by comparing
        # manifest with system state.
        password = None
        if postgres_exporter.password:
            password = postgres_exporter.password.get_secret_value()
        setup(
            ctx,
            postgres_exporter.name,
            settings,
            dsn=postgres_exporter.dsn,
            password=password,
            port=postgres_exporter.port,
        )
        if postgres_exporter.state == PostgresExporter.State.started:
            start(ctx, postgres_exporter.name, settings)
        elif postgres_exporter.state == PostgresExporter.State.stopped:
            stop(ctx, postgres_exporter.name, settings)


@task("dropping postgres_exporter service")
def drop(ctx: "BaseContext", name: str) -> None:
    """Remove a postgres_exporter service."""
    settings = available(ctx)
    if not settings:
        return
    if not exists(ctx, name):
        logger.warning("no postgres_exporter service '%s' found", name)
        return

    stop(ctx, name, settings)
    revert_setup(ctx, name, settings)


def setup_local(
    ctx: "BaseContext",
    manifest: "interface.Instance",
    settings: "PrometheusSettings",
    instance_config: "Configuration",
) -> None:
    """Setup Prometheus postgres_exporter for a local instance."""
    service = manifest.service(ServiceManifest)
    if service is None:
        return
    rolename = settings.role
    dsn = ["dbname=postgres"]
    if "port" in instance_config:
        dsn.append(f"port={instance_config.port}")
    host = instance_config.get("unix_socket_directories")
    if host:
        dsn.append(f"host={host}")
    dsn.append(f"user={rolename}")
    if not instance_config.get("ssl", False):
        dsn.append("sslmode=disable")

    instance = system.PostgreSQLInstance.system_lookup(
        ctx, (manifest.name, manifest.version)
    )
    configpath = _configpath(instance.qualname, settings)
    password_: Optional[str] = None
    if not configpath.exists():
        if service.password:
            password_ = service.password.get_secret_value()
        # Create dedicated user but only if postgres_exporter has never been initialized
        if not instance.standby:
            with instances.running(ctx, instance):
                role = interface.Role(
                    name=rolename,
                    password=password_,
                    login=True,
                    in_roles=["pg_monitor"],
                )
                if not roles.exists(ctx, instance, role.name):
                    roles.create(ctx, instance, role)
    else:
        # Get the password from config file
        password_ = password(instance.qualname, settings)

    setup(
        ctx,
        instance.qualname,
        settings,
        dsn=" ".join(dsn),
        password=password_,
        port=service.port,
    )
