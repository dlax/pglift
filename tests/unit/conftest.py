from pathlib import Path
from typing import Any, Iterator, List, Optional, Type

import pydantic
import pytest
from pgtoolkit.ctl import PGCtl

from pglift.ctx import Context
from pglift.models import interface
from pglift.models.system import Instance
from pglift.prometheus import impl as prometheus_mod
from pglift.prometheus import models as prometheus_models
from pglift.settings import Settings
from pglift.temboard import impl as temboard_mod
from pglift.temboard import models as temboard_models
from pglift.util import short_version


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--write-changes",
        action="store_true",
        default=False,
        help="Write-back changes to test data.",
    )


@pytest.fixture
def write_changes(request: Any) -> bool:
    value = request.config.option.write_changes
    assert isinstance(value, bool)
    return value


@pytest.fixture(params=[True, False], ids=["pgbackrest:yes", "pgbackrest:no"])
def pgbackrest(request: Any) -> bool:
    return request.param  # type: ignore[no-any-return]


@pytest.fixture
def need_pgbackrest(pgbackrest: bool) -> None:
    if not pgbackrest:
        pytest.skip("needs pgbackrest")


@pytest.fixture(params=[True, False], ids=["prometheus:yes", "prometheus:no"])
def prometheus(request: Any) -> bool:
    return request.param  # type: ignore[no-any-return]


@pytest.fixture
def need_prometheus(prometheus: bool) -> None:
    if not prometheus:
        pytest.skip("needs prometheus")


@pytest.fixture
def prometheus_execpath(tmp_path: Path, prometheus: bool) -> Optional[Path]:
    if not prometheus:
        return None
    execpath = tmp_path / "postgres_exporter"
    execpath.touch(0o700)
    execpath.write_text("#!/bin/sh\nexit 1\n")
    return execpath


@pytest.fixture(params=[True, False], ids=["temboard:yes", "temboard:no"])
def temboard(request: Any) -> bool:
    return request.param  # type: ignore[no-any-return]


@pytest.fixture
def need_temboard(temboard: bool) -> None:
    if not temboard:
        pytest.skip("needs temboard")


@pytest.fixture
def temboard_execpath(tmp_path: Path, temboard: bool) -> Optional[Path]:
    if not temboard:
        return None
    execpath = tmp_path / "temboard-agent"
    execpath.touch(0o700)
    execpath.write_text("#!/bin/sh\nexit 1\n")
    return execpath


@pytest.fixture
def settings(
    tmp_path: Path,
    pgbackrest: bool,
    prometheus_execpath: Optional[Path],
    temboard_execpath: Optional[Path],
) -> Settings:
    passfile = tmp_path / "pgass"
    passfile.touch()
    prometheus_settings = None
    if prometheus_execpath:
        prometheus_settings = {"execpath": prometheus_execpath}
    temboard_settings = None
    if temboard_execpath:
        temboard_settings = {"execpath": temboard_execpath}
    return Settings.parse_obj(
        {
            "prefix": str(tmp_path),
            "postgresql": {
                "auth": {
                    "local": "peer",
                    "host": "password",
                    "passfile": str(passfile),
                }
            },
            "systemd": {"unit_path": str(tmp_path / "systemd")},
            "pgbackrest": {} if pgbackrest else None,
            "prometheus": prometheus_settings,
            "temboard": temboard_settings,
        }
    )


@pytest.fixture(scope="session")
def pg_version() -> str:
    s = Settings().postgresql
    assert s.bindir
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
    return Context(settings=settings)


@pytest.fixture
def nohook(ctx: Context) -> Iterator[None]:
    unregistered = ctx.pm.unregister_all()
    yield
    for plugin in unregistered:
        ctx.pm.register(plugin)


@pytest.fixture
def composite_instance_model(ctx: Context) -> Type[interface.Instance]:
    return interface.Instance.composite(ctx.pm)


@pytest.fixture
def instance_manifest(
    composite_instance_model: Type[interface.Instance], pg_version: str
) -> interface.Instance:
    return composite_instance_model(
        name="test", version=pg_version, extensions=["passwordcheck"]
    )


def _instance(name: str, version: str, settings: Settings) -> Instance:
    # Services are looked-up in reverse order of plugin registration.
    services: List[Any] = []

    temboard = None
    if settings.temboard is not None:
        temboard_port = 2345
        temboard = temboard_models.Service(
            port=temboard_port, password=pydantic.SecretStr("dorade")
        )
        services.append(temboard)

    prometheus = None
    if settings.prometheus is not None:
        prometheus_port = 9817
        prometheus = prometheus_models.Service(
            port=prometheus_port, password=pydantic.SecretStr("truite")
        )
        services.append(prometheus)

    instance = Instance(
        name=name,
        version=version,
        settings=settings,
        services=services,
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
    (instance.datadir / "pg_hba.conf").write_text("# pg_hba.conf\n")
    (instance.datadir / "pg_ident.conf").write_text("# pg_ident.conf\n")
    confdir = instance.datadir / "conf.pglift.d"
    confdir.mkdir()
    (confdir / "user.conf").write_text(f"bonjour = on\nbonjour_name= '{name}'\n")

    if prometheus:
        assert settings.prometheus is not None
        prometheus_config = prometheus_mod._configpath(
            instance.qualname, settings.prometheus
        )
        prometheus_config.parent.mkdir(parents=True, exist_ok=True)
        prometheus_config.write_text(
            f"DATA_SOURCE_NAME=dbname=postgres port={instance.port} host={settings.postgresql.socket_directory} user=monitoring sslmode=disable password=truite\n"
            f"PG_EXPORTER_WEB_LISTEN_ADDRESS=:{prometheus.port}"
        )

    if temboard:
        assert settings.temboard is not None
        temboard_config = temboard_mod._configpath(instance.qualname, settings.temboard)
        temboard_config.parent.mkdir(parents=True, exist_ok=True)
        temboard_config.write_text(
            "\n".join(
                [
                    "[temboard]",
                    f"port = {temboard.port}",
                    "[postgresql]",
                    f"port = {instance.port}",
                    f"host = {settings.postgresql.socket_directory}",
                    "user = temboardagent",
                    "password = dorade",
                ]
            )
        )

    return instance


@pytest.fixture
def instance(pg_version: str, settings: Settings, request: Any) -> Instance:
    return _instance("test", pg_version, settings)


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
