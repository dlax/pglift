import copy
import pathlib
import shutil
import subprocess
from typing import Iterator, Set, Tuple

import pgtoolkit.conf
import port_for
import pytest
from pgtoolkit.ctl import Status

from pglift import _install
from pglift import instance as instance_mod
from pglift import model, pm
from pglift.ctx import Context
from pglift.settings import POSTGRESQL_SUPPORTED_VERSIONS, Settings

from . import configure_instance


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
    "no_service_manager_no_scheduler": {
        "service_manager": None,
        "scheduler": None,
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
    obj = copy.deepcopy(request.param)
    assert "prefix" not in obj
    obj["prefix"] = str(prefix)
    pg_obj = obj.setdefault("postgresql", {})
    assert "root" not in pg_obj
    pg_obj["root"] = str(tmp_path_factory.mktemp("postgres"))
    pgauth_obj = pg_obj.setdefault("auth", {})
    assert "passfile" not in pgauth_obj
    pgauth_obj["passfile"] = str(passfile)
    return Settings.parse_obj(obj)


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
    _install.do(ctx, env=f"SETTINGS=@{custom_settings}")
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
) -> model.InstanceSpec:
    prometheus_port = next(tmp_port_factory)
    return model.InstanceSpec(
        name="test",
        version=pg_version,
        prometheus=model.PrometheusService(prometheus_port),
        settings=settings,
    )


@pytest.fixture(scope="session")
def instance_initialized(
    ctx: Context, instance_spec: model.InstanceSpec, installed: None
) -> model.Instance:
    assert instance_mod.status(ctx, instance_spec) == Status.unspecified_datadir
    i = instance_mod.init(ctx, instance_spec)
    assert instance_mod.status(ctx, i) == Status.not_running
    return i


@pytest.fixture(scope="session")
def log_directory(tmp_path_factory):
    return tmp_path_factory.mktemp("postgres-logs")


@pytest.fixture(scope="session")
def instance(
    ctx: Context,
    instance_initialized: model.Instance,
    tmp_port_factory: Iterator[int],
    tmp_path_factory: pytest.TempPathFactory,
    log_directory: pathlib.Path,
) -> model.Instance:
    port = next(tmp_port_factory)
    i = instance_initialized
    tmp_path = tmp_path_factory.mktemp("run")
    configure_instance(
        ctx, i, port=port, socket_path=tmp_path, log_directory=str(log_directory)
    )
    return i


@pytest.fixture(scope="session")
def instance_dropped(
    ctx: Context, instance: model.Instance
) -> Tuple[model.InstanceSpec, pgtoolkit.conf.Configuration]:
    config = instance.config()
    if instance.exists():
        instance_mod.drop(ctx, instance)
    return instance.as_spec(), config
