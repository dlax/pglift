import pathlib
import socket

import pytest

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
def tmp_settings(tmp_path, passfile):
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
def installed(tmp_settings, tmp_path):
    if tmp_settings.service_manager != "systemd":
        yield
        return

    custom_settings = tmp_path / "settings.json"
    custom_settings.write_text(tmp_settings.json())
    install.do(tmp_settings, env=f"SETTINGS=@{custom_settings}")
    yield
    install.undo(tmp_settings)


@pytest.fixture
def ctx(tmp_settings):
    p = pm.PluginManager.get()
    p.trace.root.setwriter(print)
    p.enable_tracing()
    return Context(plugin_manager=p, settings=tmp_settings)


@pytest.fixture
def tmp_port():
    s = socket.socket()
    s.bind(("", 0))
    with s:
        port = s.getsockname()[1]
    return port


@pytest.fixture
def instance(ctx, installed, tmp_path, tmp_port):
    i = Instance.default_version("test", ctx=ctx)
    instance_mod.init(ctx, i)
    instance_mod.configure(ctx, i, unix_socket_directories=str(tmp_path), port=tmp_port)
    yield i
    if i.exists():
        instance_mod.drop(ctx, i)
