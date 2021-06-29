import logging
from pathlib import Path

import pytest

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
def settings(tmp_path: Path) -> Settings:
    passfile = tmp_path / "pgass"
    passfile.touch()
    return Settings.parse_obj(
        {
            "prefix": str(tmp_path),
            "postgresql": {"auth": {"passfile": str(passfile)}},
        }
    )


@pytest.fixture
def ctx(settings: Settings) -> Context:
    p = pm.PluginManager.get()
    return Context(plugin_manager=p, settings=settings)


@pytest.fixture
def instance(pg_version: str, settings: Settings) -> Instance:
    instance = Instance(name="test", version=pg_version, settings=settings)
    instance.datadir.mkdir(parents=True)
    (instance.datadir / "PG_VERSION").write_text(instance.version)
    (instance.datadir / "postgresql.conf").write_text(
        "\n".join(["port = 999", "unix_socket_directories = /socks"])
    )
    return instance


@pytest.fixture
def meminfo(tmp_path: Path) -> Path:
    fpath = tmp_path / "meminfo"
    fpath.write_text(
        "\n".join(
            [
                "MemTotal:        6022056 kB",
                "MemFree:         3226640 kB",
                "MemAvailable:    4235060 kB",
                "Buffers:          206512 kB",
            ]
        )
    )
    return fpath


@pytest.fixture
def logger() -> logging.Logger:
    return logging.getLogger()
