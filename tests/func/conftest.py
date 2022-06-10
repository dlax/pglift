import logging
import pathlib
import platform
import shutil
import subprocess
from datetime import datetime
from textwrap import dedent
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type

import pgtoolkit.conf
import port_for
import psycopg.conninfo
import pydantic
import pytest
from pgtoolkit.ctl import Status

from pglift import _install, instances, pgbackrest, prometheus, temboard
from pglift._compat import Protocol
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.settings import (
    PgBackRestSettings,
    PostgreSQLSettings,
    PostgreSQLVersion,
    PrometheusSettings,
    Settings,
    TemboardSettings,
    _postgresql_bindir_version,
    plugins,
)

from . import AuthType, execute

default_pg_version: Optional[str]
try:
    default_pg_version = _postgresql_bindir_version()[1]
except EnvironmentError:
    default_pg_version = None


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--pg-version",
        choices=list(PostgreSQLVersion),
        default=default_pg_version,
        help="Run tests with specified PostgreSQL version (default: %(default)s)",
    )
    parser.addoption(
        "--systemd",
        action="store_true",
        default=False,
        help="Run tests with systemd as service manager/scheduler",
    )
    parser.addoption(
        "--no-plugins",
        action="store_true",
        default=False,
        help="Run tests without any pglift plugin loaded.",
    )


def pytest_report_header(config: Any) -> List[str]:
    pg_version = config.getoption("--pg-version")
    return [f"postgresql: {pg_version}"]


@pytest.fixture(scope="session")
def no_plugins(request: Any) -> bool:
    value = request.config.option.no_plugins
    assert isinstance(value, bool)
    return value


@pytest.fixture(autouse=True)
def journalctl() -> Iterator[None]:
    journalctl = shutil.which("journalctl")
    if journalctl is None:
        yield
        return
    proc = subprocess.Popen([journalctl, "--user", "-f", "-n0"])
    yield
    proc.kill()


@pytest.fixture(scope="session")
def systemd_available() -> bool:
    try:
        subprocess.run(
            ["systemctl", "--user", "status"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False
    return True


@pytest.fixture(scope="session")
def pgbackrest_available(no_plugins: bool) -> bool:
    if no_plugins:
        return False
    return shutil.which("pgbackrest") is not None


@pytest.fixture(scope="session")
def prometheus_execpath(no_plugins: bool) -> Optional[pathlib.Path]:
    if no_plugins:
        return None
    for name in ("prometheus-postgres-exporter", "postgres_exporter"):
        path = shutil.which(name)
        if path is not None:
            return pathlib.Path(path)
    return None


@pytest.fixture(scope="session")
def powa_available(no_plugins: bool, pg_bindir: Tuple[pathlib.Path, str]) -> bool:
    if no_plugins:
        return False
    pg_config = pg_bindir[0] / "pg_config"
    result = subprocess.run(
        [pg_config, "--pkglibdir"],
        stdout=subprocess.PIPE,
        check=True,
        universal_newlines=True,
    )
    pkglibdir = pathlib.Path(result.stdout.strip())
    return (
        (pkglibdir / "pg_qualstats.so").exists()
        and (pkglibdir / "pg_stat_kcache.so").exists()
        and (pkglibdir / "powa.so").exists()
    )


@pytest.fixture(scope="session")
def temboard_execpath(no_plugins: bool) -> Optional[pathlib.Path]:
    if no_plugins:
        return None
    path = shutil.which("temboard-agent")
    if path is not None:
        return pathlib.Path(path)
    return None


@pytest.fixture(scope="session")
def systemd_requested(request: Any, systemd_available: bool) -> bool:
    value = request.config.getoption("--systemd")
    assert isinstance(value, bool)
    if value and not systemd_available:
        raise pytest.UsageError("systemd is not available on this system")
    return value


@pytest.fixture(scope="session", params=list(AuthType), ids=lambda v: f"auth:{v}")
def postgresql_auth(request: Any) -> AuthType:
    assert isinstance(request.param, AuthType)
    return request.param


@pytest.fixture(scope="session")
def postgresql_settings(
    tmp_path_factory: pytest.TempPathFactory, postgresql_auth: AuthType
) -> PostgreSQLSettings:
    passfile = tmp_path_factory.mktemp("home") / ".pgpass"
    if postgresql_auth == AuthType.pgpass:
        passfile.touch(mode=0o600)
    auth: Dict[str, Any] = {
        "local": "password",
        "passfile": str(passfile),
    }
    surole = {}
    if postgresql_auth == AuthType.peer:
        pass  # See also PeerAuthContext.
    elif postgresql_auth == AuthType.password_command:
        auth["password_command"] = [
            str(tmp_path_factory.mktemp("home") / "passcmd"),
            "{instance}",
        ]
    elif postgresql_auth == AuthType.pgpass:
        surole["pgpass"] = True
    else:
        raise AssertionError(f"unexpected {postgresql_auth}")
    return PostgreSQLSettings.parse_obj(
        {
            "root": str(tmp_path_factory.mktemp("postgres")),
            "auth": auth,
            "surole": surole,
        }
    )


@pytest.fixture(scope="session")
def settings(
    request: Any,
    postgresql_settings: PostgreSQLSettings,
    tmp_path_factory: pytest.TempPathFactory,
    systemd_requested: bool,
    systemd_available: bool,
    pgbackrest_available: bool,
    prometheus_execpath: Optional[pathlib.Path],
    powa_available: bool,
    temboard_execpath: Optional[pathlib.Path],
    no_plugins: bool,
) -> Settings:
    prefix = tmp_path_factory.mktemp("prefix")
    (prefix / "run" / "postgresql").mkdir(parents=True)
    obj = {
        "prefix": str(prefix),
        "run_prefix": str(tmp_path_factory.mktemp("run")),
        "postgresql": postgresql_settings.dict(),
    }
    if systemd_requested:
        obj.update({"service_manager": "systemd", "scheduler": "systemd"})

    if obj.get("service_manager") == "systemd" and not systemd_available:
        pytest.skip("systemd not functional")

    if pgbackrest_available:
        obj["pgbackrest"] = {}

    if prometheus_execpath:
        obj["prometheus"] = {"execpath": prometheus_execpath}

    if powa_available:
        obj["powa"] = {}

    if temboard_execpath:
        obj["temboard"] = {"execpath": temboard_execpath}

    try:
        s = Settings.parse_obj(obj)
    except pydantic.ValidationError as exc:
        pytest.skip(
            "; ".join(
                f"unsupported setting(s) {' '.join(map(str, e['loc']))}: {e['msg']}"
                for e in exc.errors()
            )
        )

    if no_plugins:
        to_disable = [name for name, field in plugins(s) if field is not None]
        if to_disable:
            s = s.copy(update={k: None for k in to_disable})

    return s


@pytest.fixture(scope="session")
def pg_bindir(
    request: Any, postgresql_settings: PostgreSQLSettings
) -> Tuple[pathlib.Path, str]:
    version = request.config.getoption("--pg-version")
    if version is None:
        pytest.skip("no PostgreSQL installation found")
    assert isinstance(version, str)
    assert postgresql_settings.bindir
    bindir = pathlib.Path(postgresql_settings.bindir.format(version=version))
    if not bindir.exists():
        pytest.fail(f"PostgreSQL {version} not available", pytrace=False)
    return bindir, version


@pytest.fixture(scope="session")
def pg_version(pg_bindir: Tuple[pathlib.Path, str]) -> str:
    return pg_bindir[1]


class PeerAuthContext(Context):
    @classmethod
    def site_config(cls, *parts: str) -> Optional[pathlib.Path]:
        datadir = pathlib.Path(__file__).parent / "data"
        fpath = datadir.joinpath(*parts)
        if fpath.exists():
            return fpath
        return super().site_config(*parts)


@pytest.fixture(scope="session")
def ctx(postgresql_auth: AuthType, settings: Settings) -> Context:
    logger = logging.getLogger("pglift")
    logger.setLevel(logging.DEBUG)
    cls = PeerAuthContext if postgresql_auth == AuthType.peer else Context
    context = cls(settings=settings)
    context.pm.trace.root.setwriter(print)
    context.pm.enable_tracing()
    return context


@pytest.fixture(scope="session")
def installed(ctx: Context, tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    tmp_path = tmp_path_factory.mktemp("config")
    settings = ctx.settings
    if settings.service_manager != "systemd":
        yield
        return

    custom_settings = tmp_path / "settings.json"
    custom_settings.write_text(settings.json())
    _install.do(
        ctx,
        env=f"SETTINGS=@{custom_settings}",
        header=f"# ** Test run on {platform.node()} at {datetime.now().isoformat()} **",
    )
    yield
    _install.undo(ctx)


@pytest.fixture
def pgbackrest_settings(ctx: Context) -> PgBackRestSettings:
    settings = pgbackrest.available(ctx)
    if settings is None:
        pytest.skip("pgbackrest not available")
    return settings


@pytest.fixture
def prometheus_settings(ctx: Context) -> PrometheusSettings:
    settings = prometheus.available(ctx)
    if settings is None:
        pytest.skip("prometheus not available")
    return settings


@pytest.fixture
def temboard_settings(ctx: Context) -> TemboardSettings:
    settings = temboard.available(ctx)
    if settings is None:
        pytest.skip("temboard not available")
    return settings


@pytest.fixture(scope="session")
def tmp_port_factory() -> Iterator[int]:
    """Return a generator producing available and distinct TCP ports."""

    def available_ports() -> Iterator[int]:
        used: Set[int] = set()
        while True:
            port = port_for.select_random(exclude_ports=list(used))
            used.add(port)
            yield port

    return available_ports()


@pytest.fixture(scope="session")
def surole_password(postgresql_auth: AuthType, settings: Settings) -> str:
    password = "s3kret"
    if postgresql_auth == AuthType.password_command:
        password_command = settings.postgresql.auth.password_command
        assert len(password_command) == 2
        passcmdfile = pathlib.Path(password_command[0])
        with passcmdfile.open("w") as f:
            f.write(
                dedent(
                    f"""\
                    #!/bin/sh
                    instance=$1
                    if [ ! "$instance" ]
                    then
                        echo "no instance given!!" >&2
                        exit 1
                    fi
                    echo "retrieving password for $instance..." >&2
                    echo {password}
                    """
                )
            )
        passcmdfile.chmod(0o700)

    return password


@pytest.fixture(scope="session")
def replrole_password(settings: Settings) -> str:
    return "r3pl"


@pytest.fixture(scope="session")
def prometheus_password() -> str:
    return "prom3th3us"


@pytest.fixture(scope="session")
def temboard_password() -> str:
    return "tembo@rd"


@pytest.fixture(scope="session")
def powa_password() -> str:
    return "P0w4"


@pytest.fixture(scope="session")
def composite_instance_model(ctx: Context) -> Type[interface.Instance]:
    return interface.Instance.composite(ctx.pm)


@pytest.fixture(scope="session")
def log_directory(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    return tmp_path_factory.mktemp("postgres-logs")


@pytest.fixture(scope="session")
def instance_manifest(
    ctx: Context,
    pg_version: str,
    surole_password: str,
    replrole_password: str,
    prometheus_password: str,
    temboard_password: str,
    powa_password: str,
    log_directory: pathlib.Path,
    tmp_port_factory: Iterator[int],
    composite_instance_model: Type[interface.Instance],
) -> interface.Instance:
    port = next(tmp_port_factory)
    prometheus_port = next(tmp_port_factory)
    temboard_port = next(tmp_port_factory)
    return composite_instance_model.parse_obj(
        {
            "name": "test",
            "version": pg_version,
            "state": "stopped",
            "port": port,
            "auth": {
                "host": "reject",
            },
            "configuration": {
                "log_directory": str(log_directory),
                # Keep logs to stderr in tests so that they are captured by pytest.
                "logging_collector": False,
            },
            "surole_password": surole_password,
            "replrole_password": replrole_password,
            "extensions": ["passwordcheck"],
            "prometheus": {
                "password": prometheus_password,
                "port": prometheus_port,
            },
            "powa": {"password": powa_password},
            "temboard": {
                "password": temboard_password,
                "port": temboard_port,
            },
        }
    )


@pytest.fixture(scope="session")
def instance(
    ctx: Context, instance_manifest: interface.Instance
) -> Iterator[system.Instance]:
    # Check status before initialization.
    assert instance_manifest.version is not None
    baseinstance = system.BaseInstance.get(
        instance_manifest.name, instance_manifest.version, ctx
    )
    assert instances.status(ctx, baseinstance) == Status.unspecified_datadir
    assert instances.apply(ctx, instance_manifest)
    instance = system.Instance.system_lookup(ctx, baseinstance)
    # Limit postgresql.conf to uncommented entries to reduce pytest's output
    # due to --show-locals.
    postgresql_conf = instance.datadir / "postgresql.conf"
    postgresql_conf.write_text(
        "\n".join(
            line
            for line in postgresql_conf.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
    )
    yield instance
    if instances.exists(ctx, instance.name, instance.version):
        instances.drop(ctx, instance)


@pytest.fixture(scope="session")
def standby_manifest(
    ctx: Context,
    settings: Settings,
    composite_instance_model: Type[interface.Instance],
    tmp_port_factory: Iterator[int],
    pg_version: str,
    surole_password: str,
    replrole_password: str,
    prometheus_password: str,
    temboard_password: str,
    instance: system.Instance,
) -> interface.Instance:
    primary_conninfo = psycopg.conninfo.make_conninfo(
        host=settings.postgresql.socket_directory,
        port=instance.port,
        user=settings.postgresql.replrole,
    )
    return composite_instance_model.parse_obj(
        {
            "name": "standby",
            "version": pg_version,
            "port": next(tmp_port_factory),
            "configuration": {
                # Keep logs to stderr in tests so that they are captured by pytest.
                "logging_collector": False,
            },
            "surole_password": surole_password,
            "standby": {
                "for": primary_conninfo,
                "password": replrole_password,
                "slot": "standby",
            },
            "prometheus": {
                "password": prometheus_password,
                "port": next(tmp_port_factory),
            },
            "temboard": {
                "password": temboard_password,
                "port": next(tmp_port_factory),
            },
        }
    )


@pytest.fixture(scope="session")
def standby_instance(
    ctx: Context,
    postgresql_auth: AuthType,
    standby_manifest: interface.Instance,
    instance: system.Instance,
) -> Iterator[system.Instance]:
    with instances.running(ctx, instance):
        instances.apply(ctx, standby_manifest)
    stdby_instance = system.Instance.system_lookup(
        ctx, (standby_manifest.name, standby_manifest.version)
    )
    instances.stop(ctx, stdby_instance)
    yield stdby_instance
    if instances.exists(ctx, stdby_instance.name, stdby_instance.version):
        instances.drop(ctx, stdby_instance)


def _drop_instance(
    ctx: Context, instance: system.Instance
) -> pgtoolkit.conf.Configuration:
    config = instance.config()
    if instances.exists(ctx, instance.name, instance.version):
        instances.drop(ctx, instance)
    return config


@pytest.fixture(scope="session")
def instance_dropped(
    ctx: Context, instance: system.Instance
) -> pgtoolkit.conf.Configuration:
    return _drop_instance(ctx, instance)


@pytest.fixture(scope="session")
def standby_instance_dropped(
    ctx: Context, standby_instance: system.Instance
) -> pgtoolkit.conf.Configuration:
    return _drop_instance(ctx, standby_instance)


class RoleFactory(Protocol):
    def __call__(self, name: str, options: str = "") -> None:
        ...


@pytest.fixture()
def role_factory(ctx: Context, instance: system.Instance) -> Iterator[RoleFactory]:
    rolnames = set()

    def factory(name: str, options: str = "") -> None:
        if name in rolnames:
            raise ValueError(f"'{name}' name already taken")
        execute(ctx, instance, f"CREATE ROLE {name} {options}", fetch=False)
        rolnames.add(name)

    yield factory

    for name in rolnames:
        execute(ctx, instance, f"DROP ROLE IF EXISTS {name}", fetch=False)


class DatabaseFactory(Protocol):
    def __call__(self, name: str) -> None:
        ...


@pytest.fixture()
def database_factory(
    ctx: Context, instance: system.Instance
) -> Iterator[DatabaseFactory]:
    datnames = set()

    def factory(name: str, *, owner: Optional[str] = None) -> None:
        if name in datnames:
            raise ValueError(f"'{name}' name already taken")
        sql = f"CREATE DATABASE {name}"
        if owner:
            sql += f" OWNER {owner}"
        execute(ctx, instance, sql, fetch=False, autocommit=True)
        datnames.add(name)

    yield factory

    for name in datnames:
        execute(
            ctx,
            instance,
            f"DROP DATABASE IF EXISTS {name}",
            fetch=False,
            autocommit=True,
        )
