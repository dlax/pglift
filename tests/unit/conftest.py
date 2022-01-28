from pathlib import Path
from typing import Any, Optional

import pytest
from pgtoolkit.ctl import PGCtl

from pglift import pm
from pglift import prometheus as prometheus_mod
from pglift.ctx import Context
from pglift.models import interface
from pglift.models.system import Instance, PrometheusService
from pglift.settings import Settings
from pglift.util import short_version


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--regen-test-data",
        action="store_true",
        default=False,
        help="Re-generate test data from actual results",
    )


@pytest.fixture
def regen_test_data(request: Any) -> bool:
    value = request.config.getoption("--regen-test-data")
    assert isinstance(value, bool)
    return value


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    passfile = tmp_path / "pgass"
    passfile.touch()
    return Settings.parse_obj(
        {
            "prefix": str(tmp_path),
            "postgresql": {"auth": {"passfile": str(passfile)}},
            "systemd": {"unit_path": str(tmp_path / "systemd")},
        }
    )


@pytest.fixture(scope="session")
def pg_version() -> str:
    s = Settings().postgresql
    pg_bindir_template = s.bindir
    versions = s.versions
    for version in versions:
        bindir = Path(pg_bindir_template.format(version=version))
        if bindir.exists():
            return short_version(PGCtl(bindir).version)
    else:
        pytest.skip(
            "no PostgreSQL installation found in version(s): "
            f"{', '.join(str(v) for v in versions)}"
        )


@pytest.fixture
def ctx(settings: Settings) -> Context:
    p = pm.PluginManager.get()
    return Context(plugin_manager=p, settings=settings)


@pytest.fixture
def instance_manifest(pg_version: str) -> interface.Instance:
    return interface.Instance(name="test", version=pg_version)


def _instance(
    name: str,
    version: str,
    settings: Settings,
    *,
    prometheus: Optional[PrometheusService] = None,
) -> Instance:
    instance = Instance(
        name=name, version=version, settings=settings, prometheus=prometheus
    )
    instance.datadir.mkdir(parents=True)
    (instance.datadir / "PG_VERSION").write_text(instance.version)
    (instance.datadir / "postgresql.conf").write_text(
        "\n".join(
            [
                "port = 999",
                "unix_socket_directories = /socks",
                "# backslash_quote = 'safe_encoding'",
            ]
        )
    )
    confdir = instance.datadir / "conf.pglift.d"
    confdir.mkdir()
    (confdir / "user.conf").write_text(f"bonjour = on\nbonjour_name= '{name}'\n")

    if prometheus:
        prometheus_config = prometheus_mod._configpath(
            instance.qualname, settings.prometheus
        )
        prometheus_config.parent.mkdir(parents=True)
        prometheus_config.write_text(
            f"PG_EXPORTER_WEB_LISTEN_ADDRESS=:{prometheus.port}"
        )

    return instance


@pytest.fixture(params=[True, False], ids=["prometheus=yes", "prometheus=no"])
def instance(pg_version: str, settings: Settings, request: Any) -> Instance:
    prometheus = None
    if request.param:
        prometheus_port = 9817
        prometheus = PrometheusService(port=prometheus_port)
    return _instance("test", pg_version, settings, prometheus=prometheus)


@pytest.fixture
def standby_instance(pg_version: str, settings: Settings) -> Instance:
    instance = _instance("standby", pg_version, settings)
    (
        instance.datadir
        / ("standby.signal" if int(pg_version) >= 12 else "recovery.conf")
    ).write_text("")
    (instance.datadir / "postgresql.auto.conf").write_text(
        "primary_conninfo = 'host=/tmp port=4242 user=pg'\n"
        "primary_slot_name = aslot\n"
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
