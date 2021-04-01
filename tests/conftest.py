import pytest

from pglib import install
from pglib import instance as instance_mod
from pglib import pm
from pglib.ctx import Context
from pglib.model import Instance
from pglib.settings import Settings


@pytest.fixture
def tmp_settings(tmp_path):

    pgbackrest_root = tmp_path / "pgbackrest"
    pgbackrest_root.mkdir()

    prometheus_root = tmp_path / "prometheus"
    prometheus_root.mkdir()

    return Settings.parse_obj(
        {
            "postgresql": {
                "root": str(tmp_path),
                "pid_directory": str(tmp_path / "run"),
            },
            "pgbackrest": {
                "configpath": str(
                    pgbackrest_root / "{instance.version}" / "pgbackrest.conf"
                ),
                "directory": str(tmp_path / "{instance.version}" / "backups"),
                "logpath": str(pgbackrest_root / "{instance.version}" / "logs"),
            },
            "prometheus": {
                "configpath": str(
                    prometheus_root / "{instance.version}" / "postgres_exporter.conf"
                ),
                "queriespath": str(
                    prometheus_root / "{instance.version}" / "queries.yaml"
                ),
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
    return Context(plugin_manager=p, settings=tmp_settings)


@pytest.fixture
def instance(ctx, installed, tmp_path):
    i = Instance.default_version("test", ctx=ctx)
    instance_mod.init(ctx, i)
    instance_mod.configure(ctx, i, unix_socket_directories=str(tmp_path))
    return i
