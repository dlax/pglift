import copy
import logging
import pathlib
import platform
import shutil
import subprocess
from datetime import datetime
from typing import Iterator, Set, Tuple

import pgtoolkit.conf
import port_for
import pydantic
import pytest
from pgtoolkit.ctl import Status

from pglift import _install
from pglift import instance as instance_mod
from pglift import pm
from pglift.ctx import Context
from pglift.models import system
from pglift.settings import POSTGRESQL_SUPPORTED_VERSIONS, Settings
from pglift.util import xdg_runtime_dir

from . import configure_instance, execute

logging.basicConfig(level="DEBUG")


@pytest.fixture(autouse=True)
def journalctl():
    journalctl = shutil.which("journalctl")
    if journalctl is None:
        yield
        return
    proc = subprocess.Popen([journalctl, "--user", "-f"])
    yield
    proc.kill()


settings_by_id = {
    "defaults": {},
    "systemd": {
        "service_manager": "systemd",
        "scheduler": "systemd",
    },
    "postgresql_password_auth__surole_use_pgpass": {
        "postgresql": {
            "auth": {
                "local": "password",
                "host": "reject",
            },
            "surole": {
                "password": "s3kret",
                "pgpass": True,
            },
        },
    },
    "postgresql_password_auth__surole_no_pgpass": {
        "postgresql": {
            "auth": {
                "local": "password",
                "host": "reject",
            },
            "surole": {
                "password": "s3kret",
                "pgpass": False,
            },
        },
    },
}
ids, params = zip(*settings_by_id.items())
ids = tuple(f"settings:{i}" for i in ids)


@pytest.fixture(scope="session", params=params, ids=ids)
def settings(request, tmp_path_factory):
    passfile = tmp_path_factory.mktemp("home") / ".pgpass"
    passfile.touch(mode=0o600)
    passfile.write_text("#hostname:port:database:username:password\n")

    prefix = tmp_path_factory.mktemp("prefix")
    (prefix / "run" / "postgresql").mkdir(parents=True)
    obj = copy.deepcopy(request.param)
    assert "prefix" not in obj
    obj["prefix"] = str(prefix)
    pg_obj = obj.setdefault("postgresql", {})
    assert "root" not in pg_obj
    pg_obj["root"] = str(tmp_path_factory.mktemp("postgres"))
    pgauth_obj = pg_obj.setdefault("auth", {})
    assert "passfile" not in pgauth_obj
    pgauth_obj["passfile"] = str(passfile)
    obj.setdefault("systemd", {}).setdefault(
        "unit_path", str(xdg_runtime_dir() / "systemd" / "user.control")
    )
    try:
        return Settings.parse_obj(obj)
    except pydantic.ValidationError as exc:
        pytest.skip(
            "; ".join(
                f"unsupported setting(s) {' '.join(e['loc'])}: {e['msg']}"
                for e in exc.errors()
            )
        )


@pytest.fixture(
    scope="session",
    params=POSTGRESQL_SUPPORTED_VERSIONS,
    ids=lambda v: f"postgresql:{v}",
)
def pg_version(request, settings):
    version = request.param
    if not pathlib.Path(settings.postgresql.bindir.format(version=version)).exists():
        pytest.skip(f"PostgreSQL {version} not available")
    return version


@pytest.fixture(scope="session")
def ctx(settings):
    p = pm.PluginManager.get()
    p.trace.root.setwriter(print)
    p.enable_tracing()
    return Context(plugin_manager=p, settings=settings)


@pytest.fixture(scope="session")
def installed(ctx, tmp_path_factory):
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
def instance_spec(
    pg_version: str, settings: Settings, tmp_port_factory: Iterator[int]
) -> system.InstanceSpec:
    prometheus_port = next(tmp_port_factory)
    return system.InstanceSpec(
        name="test",
        version=pg_version,
        prometheus=system.PrometheusService(prometheus_port),
        settings=settings,
        standby=None,
    )


@pytest.fixture(scope="session")
def instance_initialized(
    ctx: Context, instance_spec: system.InstanceSpec, installed: None
) -> system.InstanceSpec:
    assert instance_mod.status(ctx, instance_spec) == Status.unspecified_datadir
    instance_mod.init(ctx, instance_spec)
    assert instance_mod.status(ctx, instance_spec) == Status.not_running
    return instance_spec


@pytest.fixture(scope="session")
def log_directory(tmp_path_factory):
    return tmp_path_factory.mktemp("postgres-logs")


@pytest.fixture(scope="session")
def instance(
    ctx: Context,
    instance_initialized: system.InstanceSpec,
    tmp_port_factory: Iterator[int],
    log_directory: pathlib.Path,
) -> system.Instance:
    port = next(tmp_port_factory)
    i = instance_initialized
    configure_instance(ctx, i, port=port, log_directory=str(log_directory))
    return system.Instance.system_lookup(ctx, i)


@pytest.fixture(scope="session")
def instance_dropped(
    ctx: Context, instance: system.Instance
) -> Tuple[system.InstanceSpec, pgtoolkit.conf.Configuration]:
    config = instance.config()
    if instance.exists():
        instance_mod.drop(ctx, instance)
    return instance.as_spec(), config


@pytest.fixture(scope="module")
def role_factory(ctx, instance):
    rolnames = set()

    def factory(name: str, options: str = "") -> None:
        if name in rolnames:
            raise ValueError(f"'{name}' name already taken")
        execute(ctx, instance, f"CREATE ROLE {name} {options}", fetch=False)
        rolnames.add(name)

    yield factory

    for name in rolnames:
        execute(ctx, instance, f"DROP ROLE IF EXISTS {name}", fetch=False)


@pytest.fixture(scope="module")
def database_factory(ctx, instance):
    datnames = set()

    def factory(name: str) -> None:
        if name in datnames:
            raise ValueError(f"'{name}' name already taken")
        execute(
            ctx,
            instance,
            f"CREATE DATABASE {name}",
            fetch=False,
            autocommit=True,
        )
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
