import copy
import pathlib
import shutil
import subprocess
from typing import Iterator, Set

import port_for
import pytest
from pgtoolkit.ctl import Status

from pglift import install
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
def installed(settings, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("config")
    if settings.service_manager != "systemd":
        yield
        return

    custom_settings = tmp_path / "settings.json"
    custom_settings.write_text(settings.json())
    install.do(settings, env=f"SETTINGS=@{custom_settings}")
    yield
    install.undo(settings)


@pytest.fixture(scope="session")
def ctx(settings):
    p = pm.PluginManager.get()
    p.trace.root.setwriter(print)
    p.enable_tracing()
    return Context(plugin_manager=p, settings=settings)


@pytest.fixture(scope="session")
def tmp_port_factory():
    """Return a generator producing available and distinct TCP ports."""

    def available_ports() -> Iterator[int]:
        used: Set[int] = set()
        while True:
            port = port_for.select_random(exclude_ports=list(used))
            used.add(port)
            yield port

    return available_ports()


@pytest.fixture(scope="session")
def instance_obj(pg_version, settings, tmp_port_factory):
    prometheus_port = next(tmp_port_factory)
    return model.Instance(
        name="test",
        version=pg_version,
        prometheus=model.PrometheusService(prometheus_port),
        settings=settings,
    )


@pytest.fixture(scope="session")
def instance_initialized(ctx, instance_obj, installed):
    i = instance_obj
    assert instance_mod.status(ctx, i) == Status.unspecified_datadir
    rv = instance_mod.init(ctx, i)
    assert instance_mod.status(ctx, i) == Status.not_running
    assert rv
    return i


@pytest.fixture(scope="session")
def log_directory(tmp_path_factory):
    return tmp_path_factory.mktemp("postgres-logs")


@pytest.fixture(scope="session")
def instance(
    ctx, instance_initialized, tmp_port_factory, tmp_path_factory, log_directory
):
    port = next(tmp_port_factory)
    i = instance_initialized
    tmp_path = tmp_path_factory.mktemp("run")
    configure_instance(
        ctx, i, port=port, socket_path=tmp_path, log_directory=str(log_directory)
    )
    assert i.config()
    return i


@pytest.fixture(scope="session")
def instance_dropped(ctx, instance):
    config = instance.config()
    assert config
    if instance.exists():
        instance_mod.drop(ctx, instance)
    return instance, config
