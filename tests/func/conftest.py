import shutil
import socket
import subprocess

import pytest
from pgtoolkit.ctl import Status

from pglift import install
from pglift import instance as instance_mod
from pglift import pm
from pglift.ctx import Context
from pglift.model import Instance
from pglift.settings import Settings


@pytest.fixture(autouse=True)
def journalctl():
    journalctl = shutil.which("journalctl")
    if journalctl is None:
        yield
        return
    proc = subprocess.Popen([journalctl, "--user", "-f"])
    yield
    proc.kill()


@pytest.fixture(scope="session")
def settings(tmp_path_factory):
    passfile = tmp_path_factory.mktemp("home") / ".pgpass"
    prefix = tmp_path_factory.mktemp("prefix")
    return Settings.parse_obj(
        {
            "prefix": str(prefix),
            "postgresql": {
                "root": str(prefix / "postgres"),
                "auth": {
                    "local": "password",
                    "host": "reject",
                    "passfile": str(passfile),
                },
                "surole": {"password": "s3kret", "pgpass": True},
            },
        }
    )


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
def tmp_port():
    s = socket.socket()
    s.bind(("", 0))
    with s:
        port = s.getsockname()[1]
    return port


@pytest.fixture(scope="session")
def instance_obj(ctx):
    return Instance.default_version("test", ctx=ctx)


@pytest.fixture(scope="session")
def instance_initialized(ctx, instance_obj, installed):
    i = instance_obj
    assert instance_mod.status(ctx, i) == Status.unspecified_datadir
    rv = instance_mod.init(ctx, i)
    assert instance_mod.status(ctx, i) == Status.not_running
    assert rv
    return i


@pytest.fixture(scope="session")
def instance_configured(ctx, instance_initialized, tmp_port, tmp_path_factory):
    i = instance_initialized
    tmp_path = tmp_path_factory.mktemp("run")
    instance_mod.configure(ctx, i, unix_socket_directories=str(tmp_path), port=tmp_port)
    assert i.config()
    return i


@pytest.fixture(scope="session")
def instance_auth_configured(ctx, instance_configured, tmp_port, tmp_path_factory):
    i = instance_configured

    passfile = None
    if ctx.settings.postgresql.surole.pgpass:
        passfile = ctx.settings.postgresql.auth.passfile
        assert not passfile.exists()

    instance_mod.configure_auth(ctx, i)
    return i


@pytest.fixture(scope="session")
def instance(ctx, instance_auth_configured, installed):
    i = instance_auth_configured
    return i


@pytest.fixture(scope="session")
def instance_dropped(ctx, instance):
    if instance.exists():
        instance_mod.drop(ctx, instance)
    return instance
