import json
import os
import shutil
from pathlib import Path, PosixPath
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Type, TypeVar, Union

import yaml
from pydantic import BaseSettings, Field, SecretStr, root_validator, validator
from pydantic.env_settings import SettingsSourceCallable
from pydantic.fields import ModelField
from typing_extensions import Literal, TypedDict

from . import __name__ as pkgname
from . import datapath
from .types import Role
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


POSTGRESQL_SUPPORTED_VERSIONS = ["13", "12", "11", "10"]


class PostgreSQLVersionSettings(BaseSettings):
    bindir: Path


def _postgresql_bindir() -> str:
    usrdir = Path("/usr")
    for version in POSTGRESQL_SUPPORTED_VERSIONS:
        # Debian packages
        if (usrdir / "lib" / "postgresql" / version).exists():
            return str(usrdir / "lib" / "postgresql" / "{version}" / "bin")

        # RPM packages from the PGDG
        if (usrdir / f"pgsql-{version}").exists():
            return str(usrdir / "pgsql-{version}" / "bin")
    else:
        raise EnvironmentError("no PostgreSQL installation found")


AuthMethod = Union[
    Literal["trust"],
    Literal["reject"],
    Literal["md5"],
    Literal["password"],
    Literal["scram-sha-256"],
    Literal["gss"],
    Literal["sspi"],
    Literal["ident"],
    Literal["peer"],
    Literal["pam"],
    Literal["ldap"],
    Literal["radius"],
    Literal["cert"],
]


class AuthEnviron(TypedDict, total=False):
    PGPASSWORD: str
    PGPASSFILE: str


@frozen
class AuthSettings(BaseSettings):
    local: AuthMethod = "trust"
    """Default authentication method for local TCP/IP connections"""
    host: AuthMethod = "trust"
    """Default authentication method for local-socket connections."""

    passfile: Path = Path.home() / ".pgpass"
    """Path to .pgpass file."""

    def libpq_environ(self, role: Role) -> AuthEnviron:
        """Return a dict with libpq environment variables for `role`
        authentication.

        >>> class MyRole(BaseSettings):
        ...     name: str
        ...     password: Optional[SecretStr] = None
        ...     pgpass: bool = False

        >>> s = AuthSettings.parse_obj({"passfile": "/srv/pg/.pgpass"})
        >>> s.libpq_environ(MyRole(name="bob"))
        {}
        >>> s.libpq_environ(MyRole(name="bob", password=SecretStr("secret")))
        {'PGPASSWORD': 'secret'}
        >>> s.libpq_environ(MyRole(name="bob", password=SecretStr("whatever"), pgpass=True))
        {'PGPASSFILE': '/srv/pg/.pgpass'}
        """
        env: AuthEnviron = {}
        if role.password:
            if role.pgpass:
                env["PGPASSFILE"] = str(self.passfile)
            else:
                env["PGPASSWORD"] = role.password.get_secret_value()
        return env


@frozen
class InitdbSettings(BaseSettings):
    """Settings for initdb step of a PostgreSQL instance."""

    locale: Optional[str] = "C"
    """Instance locale as used by initdb."""

    data_checksums: bool = False
    """Use checksums on data pages."""


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
    def default_version_in_supported_versions(cls, v: Optional[str]) -> Optional[str]:
        if v and v not in POSTGRESQL_SUPPORTED_VERSIONS:
            raise ValueError(f"unsupported default version: {v}")
        return v

    root: DataPath = DataPath("pgsql")
    """Root directory for all managed instances."""

    initdb: InitdbSettings = InitdbSettings()

    auth: AuthSettings = AuthSettings()

    @frozen
    class SuRole(BaseSettings):
        name: str = "postgres"
        password: Optional[SecretStr] = None
        pgpass: bool = False
        """Whether to store the password in .pgpass file."""

        class Config:
            env_prefix = "postgresql_surole_"

            @classmethod
            def customise_sources(
                cls,
                init_settings: SettingsSourceCallable,
                env_settings: SettingsSourceCallable,
                file_secret_settings: SettingsSourceCallable,
            ) -> Tuple[SettingsSourceCallable, ...]:
                return (init_settings, env_settings)

    surole: SuRole = SuRole()
    """Instance super-user role."""

    datadir: str = "data"
    """Path segment from instance base directory to PGDATA directory."""

    waldir: str = "wal"
    """Path segment from instance base directory to WAL directory."""

    pid_directory: RunPath = RunPath("postgresql")
    """Path to directory where postgres process PID file will be written."""

    socket_directory: RunPath = RunPath("postgresql")
    """Path to directory where postgres unix socket will be written."""


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

    spoolpath: DataPath = DataPath(
        "pgbackrest/{instance.version}-{instance.name}/spool"
    )
    """Spool path."""

    lockpath: RunPath = RunPath("pgbackrest/{instance.version}-{instance.name}/lock")
    """Path where lock files are stored."""


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

    pid_file: RunPath = RunPath("prometheus/{instance.version}-{instance.name}.pid")
    """Path to directory where postgres_exporter process PID file will be written."""


@frozen
class SystemdSettings(BaseSettings):

    unit_path: Path = xdg_data_home() / "systemd" / "user"
    """Base path where systemd units will be installed."""


def yaml_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """Load settings values 'settings.yaml' file if found in data directory."""
    fpath = datapath / "settings.yaml"
    if not fpath.exists():
        return {}
    with fpath.open() as f:
        settings = yaml.safe_load(f)
    if not isinstance(settings, dict):
        raise TypeError(f"expecting an object while loading settings from {fpath}")
    return settings


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


@frozen
class Settings(BaseSettings):

    postgresql: PostgreSQLSettings = PostgreSQLSettings()
    pgbackrest: PgBackRestSettings = PgBackRestSettings()
    prometheus: PrometheusSettings = PrometheusSettings()
    systemd: SystemdSettings = SystemdSettings()

    service_manager: Optional[Literal["systemd"]] = None
    scheduler: Optional[Literal["systemd"]] = None

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
                child_values = child.dict()
                child_values.update(update)
                values[key] = child.__class__(**child_values)
        return values

    @validator("service_manager", "scheduler", always=True)
    def __validate_systemd_(
        cls, v: Optional[Literal["systemd"]], field: ModelField
    ) -> Optional[str]:
        if v == "systemd" and shutil.which("systemctl") is None:
            raise ValueError(
                f"systemctl command not found, cannot use systemd for '{field.alias}' setting"
            )
        return v

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
                yaml_settings_source,
                json_config_settings_source,
            )


if __name__ == "__main__":
    s = Settings()
    print(s.json(indent=2))
