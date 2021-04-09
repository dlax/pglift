import json
import os
import shutil
from pathlib import Path, PosixPath
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pydantic import BaseSettings, Field, root_validator
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
    PosixPath('/home/.../.local/share/pglib')
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
    basedir = Path("var/lib")


@frozen
class PostgreSQLSettings(BaseSettings):
    """Settings for PostgreSQL."""

    versions: List[str] = ["13", "12", "11", "10", "9.6"]
    """Available PostgreSQL versions."""

    root: DataPath = DataPath("pgsql")
    """Root directory for all managed instances."""

    locale: Optional[str] = "C"
    """Instance locale as used by initdb."""

    data_checksums: bool = False
    """Use checksums on data pages."""

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

    initdb_auth: Optional[
        Tuple[Union[Literal["md5"], Literal["scram-sha-256"]], Optional[Path]]
    ]
    """Auth method and pwfile to be used by initdb.
    Examples:
      - None: `trust` method is used in pg_hba.conf,
      - ('md5', None): user is asked a password,
      - ('md5', Path(/path/to/surole_pwd)): the file is read by initdb for the
        password.
    """

    class Config:
        env_prefix = f"{pkgname}_postgresql_"


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

    class Config:
        env_prefix = f"{pkgname}_pgbackrest_"


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

    class Config:
        env_prefix = f"{pkgname}_prometheus_"


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
        env_prefix = f"{pkgname}_"

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            return (
                init_settings,
                json_config_settings_source,
                env_settings,
                file_secret_settings,
            )


SETTINGS = Settings()


if __name__ == "__main__":

    print(SETTINGS.json(indent=2))
