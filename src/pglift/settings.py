import json
import os
import shutil
from pathlib import Path, PosixPath
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Type, TypeVar, Union

from pydantic import BaseSettings, Field, root_validator, validator
from pydantic.env_settings import SettingsSourceCallable
from typing_extensions import Literal

from . import __name__ as pkgname
from .util import xdg_data_home

T = TypeVar("T", bound=BaseSettings)


def frozen(cls: Type[T]) -> Type[T]:
    cls.Config.frozen = True
    return cls


def default_prefix(uid: int) -> Path:
    """Return the default path prefix for 'uid'.

    >>> default_prefix(0)
    PosixPath('/')
    >>> default_prefix(42)  # doctest: +ELLIPSIS
    PosixPath('/home/.../.local/share/pglift')
    """
    if uid == 0:
        return Path("/")
    return xdg_data_home() / pkgname


class PrefixedPath(PosixPath):
    basedir = Path("")

    @classmethod
    def __get_validators__(cls) -> Iterator[Callable[[Path], "PrefixedPath"]]:
        yield cls.validate

    @classmethod
    def validate(cls, value: Path) -> "PrefixedPath":
        if not isinstance(value, cls):
            value = cls(value)
        return value

    def prefix(self, prefix: Path) -> Path:
        """Return the path prefixed if is not yet absolute.

        >>> PrefixedPath("documents").prefix("/home/alice")
        PosixPath('/home/alice/documents')
        >>> PrefixedPath("/root").prefix("/whatever")
        PosixPath('/root')
        """
        if self.is_absolute():
            return Path(self)
        return prefix / self.basedir / self


class ConfigPath(PrefixedPath):
    basedir = Path("etc")


class RunPath(PrefixedPath):
    basedir = Path("run")


class DataPath(PrefixedPath):
    basedir = Path("srv")


POSTGRESQL_SUPPORTED_VERSIONS = ["13", "12", "11", "10", "9.6"]


class PostgreSQLVersionSettings(BaseSettings):
    bindir: Path


def _postgresql_bindir() -> str:
    prefix = Path("/usr/lib")
    for name in ("postgresql", "pgsql"):
        if (prefix / name).exists():
            return str(prefix / name / "{version}" / "bin")
    else:
        raise EnvironmentError("no PostgreSQL installation found")


@frozen
class InitdbSettings(BaseSettings):
    """Settings for initdb step of a PostgreSQL instance."""

    locale: Optional[str] = "C"
    """Instance locale as used by initdb."""

    data_checksums: bool = False
    """Use checksums on data pages."""

    auth: Optional[
        Tuple[Union[Literal["md5"], Literal["scram-sha-256"]], Optional[Path]]
    ]
    """Auth method and optional pwfile.

    Examples:
      - None: `trust` method is used in pg_hba.conf,
      - ('md5', None): user is asked a password,
      - ('md5', Path(/path/to/surole_pwd)): the file is read by initdb for the
        password.
    """


@frozen
class PostgreSQLSettings(BaseSettings):
    """Settings for PostgreSQL."""

    bindir: str = _postgresql_bindir()
    """Default PostgreSQL bindir, templated by version."""

    versions: Dict[str, PostgreSQLVersionSettings] = Field(default_factory=lambda: {})
    """Available PostgreSQL versions."""

    @root_validator
    def set_versions(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        bindir = values["bindir"]
        pgversions = values["versions"]
        for version in POSTGRESQL_SUPPORTED_VERSIONS:
            if version not in pgversions:
                pgversions[version] = PostgreSQLVersionSettings(
                    bindir=bindir.format(version=version)
                )
        return values

    default_version: Optional[str] = None
    """Default PostgreSQL version to use, if unspecified."""

    @validator("default_version")
    def default_version_in_supported_versions(cls, v: Optional[str]) -> None:
        if v and v not in POSTGRESQL_SUPPORTED_VERSIONS:
            raise ValueError(f"unsupported default version: {v}")

    root: DataPath = DataPath("pgsql")
    """Root directory for all managed instances."""

    initdb: InitdbSettings = InitdbSettings()

    surole: str = "postgres"
    """User name of instance super-user."""

    instancedir: str = "{version}/{instance}"
    """Path segment to instance base directory relative to `root` path."""

    datadir: str = "data"
    """Path segment from instance base directory to PGDATA directory."""

    waldir: str = "wal"
    """Path segment from instance base directory to WAL directory."""

    pid_directory: RunPath = RunPath("postgresql")
    """Path to directory where postgres process PID file will be written."""


@frozen
class PgBackRestSettings(BaseSettings):
    """Settings for pgBackRest."""

    execpath: Path = Path("/usr/bin/pgbackrest")
    """Path to the pbBackRest executable."""

    configpath: ConfigPath = ConfigPath(
        "pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf"
    )
    """Path to the config file."""

    directory: DataPath = DataPath("pgbackrest/{instance.version}-{instance.name}")
    """Path to the directory where backups are stored."""

    logpath: DataPath = DataPath("pgbackrest/{instance.version}-{instance.name}/logs")
    """Path where log files are stored."""


@frozen
class PrometheusSettings(BaseSettings):
    """Settings for Prometheus postgres_exporter"""

    execpath: Path = Path("/usr/bin/prometheus-postgres-exporter")
    """Path to the postgres_exporter executable."""

    configpath: ConfigPath = ConfigPath(
        "prometheus/postgres_exporter-{instance.version}-{instance.name}.conf"
    )
    """Path to the config file."""

    queriespath: ConfigPath = ConfigPath(
        "prometheus/postgres_exporter_queries-{instance.version}-{instance.name}.yaml"
    )
    """Path to the queries file."""
    port: int = 9187
    """TCP port for the web interface and telemetry."""


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """Load settings values from 'SETTINGS' environment variable.

    If this variable has a value starting with @, it is interpreted as a path
    to a JSON file. Otherwise, a JSON serialization is expected.
    """
    env_settings = os.getenv("SETTINGS")
    if not env_settings:
        return {}
    if env_settings.startswith("@"):
        config = Path(env_settings[1:])
        encoding = settings.__config__.env_file_encoding
        # May raise FileNotFoundError, which is okay here.
        env_settings = config.read_text(encoding)
    return json.loads(env_settings)  # type: ignore[no-any-return]


def maybe_systemd() -> Optional[Literal["systemd"]]:
    if shutil.which("systemctl") is not None:
        return "systemd"
    return None


@frozen
class Settings(BaseSettings):

    postgresql: PostgreSQLSettings = PostgreSQLSettings()
    pgbackrest: PgBackRestSettings = PgBackRestSettings()
    prometheus: PrometheusSettings = PrometheusSettings()

    service_manager: Optional[Literal["systemd"]] = Field(default_factory=maybe_systemd)
    scheduler: Optional[Literal["systemd"]] = Field(default_factory=maybe_systemd)

    prefix: Path = default_prefix(os.getuid())
    """Path prefix for configuration and data files."""

    @root_validator
    def __prefix_paths(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Prefix child settings fields with the global 'prefix'."""
        prefix = values["prefix"]
        for key, child in values.items():
            if not isinstance(child, BaseSettings):
                continue
            update = {
                fn: getattr(child, fn).prefix(prefix)
                for fn, mf in child.__fields__.items()
                # mf.types_ may be a typing.* class, which is not a type.
                if isinstance(mf.type_, type) and issubclass(mf.type_, PrefixedPath)
            }
            if update:
                values[key] = child.copy(update=update)
        return values

    class Config:
        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            return (
                init_settings,
                env_settings,
                json_config_settings_source,
                file_secret_settings,
            )


SETTINGS = Settings()


if __name__ == "__main__":

    print(SETTINGS.json(indent=2))
