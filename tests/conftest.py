import socket

import pytest

from pglift import install
from pglift import instance as instance_mod
from pglift import pm
from pglift.ctx import Context
from pglift.model import Instance
from pglift.settings import Settings


@pytest.fixture
def tmp_settings(tmp_path):
    return Settings.parse_obj(
        {
            "prefix": str(tmp_path),
            "postgresql": {"root": str(tmp_path / "postgres")},
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
