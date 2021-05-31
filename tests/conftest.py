import pathlib
import socket

import pytest
from pgtoolkit.ctl import Status

from pglift import install
from pglift import instance as instance_mod
from pglift import pm
from pglift.ctx import Context
from pglift.model import Instance
from pglift.settings import Settings


def pytest_addoption(parser, pluginmanager):
    parser.addoption(
        "--regen-test-data",
        action="store_true",
        default=False,
        help="Re-generate test data from actual results",
    )


@pytest.fixture
def regen_test_data(request):
    return request.config.getoption("--regen-test-data")


@pytest.fixture
def datadir():
    return pathlib.Path(__file__).parent / "data"


@pytest.fixture
def passfile(tmp_path):
    return tmp_path / ".pgpass"


@pytest.fixture
def settings(tmp_path, passfile):
    return Settings.parse_obj(
        {
            "prefix": str(tmp_path),
            "postgresql": {
                "root": str(tmp_path / "postgres"),
                "auth": {
                    "local": "password",
                    "host": "reject",
                    "passfile": str(passfile),
                },
                "surole": {"password": "s3kret", "pgpass": True},
            },
        }
    )


@pytest.fixture
def installed(settings, tmp_path):
    if settings.service_manager != "systemd":
        yield
        return

    custom_settings = tmp_path / "settings.json"
    custom_settings.write_text(settings.json())
    install.do(settings, env=f"SETTINGS=@{custom_settings}")
    yield
    install.undo(settings)


@pytest.fixture
def ctx(settings):
    p = pm.PluginManager.get()
    p.trace.root.setwriter(print)
    p.enable_tracing()
    return Context(plugin_manager=p, settings=settings)


@pytest.fixture
def tmp_port():
    s = socket.socket()
    s.bind(("", 0))
    with s:
        port = s.getsockname()[1]
    return port


@pytest.fixture
def instance_obj(ctx):
    return Instance.default_version("test", ctx=ctx)


@pytest.fixture
def instance_initialized(ctx, instance_obj, installed):
    i = instance_obj
    assert instance_mod.status(ctx, i) == Status.unspecified_datadir
    rv = instance_mod.init(ctx, i)
    assert instance_mod.status(ctx, i) == Status.not_running
    assert rv
    return i


@pytest.fixture
def instance_configured(ctx, instance_initialized, tmp_port, tmp_path_factory):
    i = instance_initialized
    tmp_path = tmp_path_factory.mktemp("run")
    instance_mod.configure(ctx, i, unix_socket_directories=str(tmp_path), port=tmp_port)
    assert i.config()
    return i


@pytest.fixture
def instance_auth_configured(ctx, instance_configured, tmp_port, tmp_path_factory):
    i = instance_configured

    passfile = None
    if ctx.settings.postgresql.surole.pgpass:
        passfile = ctx.settings.postgresql.auth.passfile
        assert not passfile.exists()

    instance_mod.configure_auth(ctx, i)
    return i


@pytest.fixture
def instance(ctx, instance_auth_configured, installed):
    i = instance_auth_configured
    yield i
    if i.exists():
        instance_mod.drop(ctx, i)
