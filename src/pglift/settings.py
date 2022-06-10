import enum
import grp
import json
import os
import pwd
import shutil
from pathlib import Path, PosixPath
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import yaml
from pydantic import BaseSettings, Field, FilePath, root_validator, validator
from pydantic.fields import ModelField
from pydantic.utils import lenient_issubclass

from . import __name__ as pkgname
from . import exceptions, types, util
from ._compat import Literal

try:
    from pydantic.env_settings import SettingsSourceCallable
except ImportError:
    SettingsSourceCallable = Callable[[BaseSettings], Dict[str, Any]]  # type: ignore[misc]

if TYPE_CHECKING:
    from .ctx import BaseContext
    from .models.system import BaseInstance


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
    return util.xdg_data_home() / pkgname


def default_run_prefix(uid: int) -> Path:
    """Return the default run path prefix for 'uid'."""
    base = Path("/run") if uid == 0 else util.xdg_runtime_dir(uid)
    return base / pkgname


def default_systemd_unit_path(uid: int) -> Path:
    """Return the default systemd unit path for 'uid'.

    >>> default_systemd_unit_path(0)
    PosixPath('/etc/systemd/system')
    >>> default_systemd_unit_path(42)  # doctest: +ELLIPSIS
    PosixPath('/home/.../.local/share/systemd/user')
    """
    if uid == 0:
        return Path("/etc/systemd/system")
    return util.xdg_data_home() / "systemd" / "user"


def default_sysuser() -> Tuple[str, str]:
    pwentry = pwd.getpwuid(os.getuid())
    grentry = grp.getgrgid(pwentry.pw_gid)
    return pwentry.pw_name, grentry.gr_name


class PrefixedPath(PosixPath):
    basedir = Path("")
    key = "prefix"

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
    basedir = Path("")
    key = "run_prefix"


class DataPath(PrefixedPath):
    basedir = Path("srv")


class LogPath(PrefixedPath):
    basedir = Path("log")


class PluginSettings(BaseSettings):
    """Settings class for plugins."""


# List of extensions supported by pglift
# The value is a tuple with two items:
#  - the first one tells if the module needs to be added to shared_preload_libraries
#  - the second one tells if the module is an extension (used with CREATE EXTENSION…)
EXTENSIONS_CONFIG: Dict[types.Extension, Tuple[bool, bool]] = {
    types.Extension.btree_gist: (False, True),
    types.Extension.passwordcheck: (True, False),
    types.Extension.pg_qualstats: (True, True),
    types.Extension.pg_stat_kcache: (True, True),
    types.Extension.pg_stat_statements: (True, True),
    types.Extension.powa: (False, True),
    types.Extension.unaccent: (False, True),
}


class PostgreSQLVersion(types.StrEnum):
    """PostgreSQL version"""

    v14 = "14"
    v13 = "13"
    v12 = "12"
    v11 = "11"
    v10 = "10"


class PostgreSQLVersionSettings(BaseSettings):
    bindir: Path


def _postgresql_bindir_version() -> Tuple[str, str]:
    usrdir = Path("/usr")
    for version in PostgreSQLVersion:
        # Debian packages
        if (usrdir / "lib" / "postgresql" / version).exists():
            return str(usrdir / "lib" / "postgresql" / "{version}" / "bin"), version

        # RPM packages from the PGDG
        if (usrdir / f"pgsql-{version}").exists():
            return str(usrdir / "pgsql-{version}" / "bin"), version
    else:
        raise EnvironmentError("no PostgreSQL installation found")


bindir: Optional[str]
try:
    bindir = _postgresql_bindir_version()[0]
except EnvironmentError:
    bindir = None


class AuthLocalMethod(types.AutoStrEnum):
    """Local authentication method"""

    trust = enum.auto()
    reject = enum.auto()
    md5 = enum.auto()
    password = enum.auto()
    scram_sha_256 = "scram-sha-256"
    gss = enum.auto()
    sspi = enum.auto()
    ident = enum.auto()
    peer = enum.auto()
    pam = enum.auto()
    ldap = enum.auto()
    radius = enum.auto()
    cert = enum.auto()


class AuthHostMethod(types.AutoStrEnum):
    """Host authentication method"""

    trust = enum.auto()
    reject = enum.auto()
    md5 = enum.auto()
    password = enum.auto()
    scram_sha_256 = "scram-sha-256"
    gss = enum.auto()
    sspi = enum.auto()
    ident = enum.auto()
    pam = enum.auto()
    ldap = enum.auto()
    radius = enum.auto()
    cert = enum.auto()


@frozen
class AuthSettings(BaseSettings):
    """PostgreSQL authentication settings."""

    class Config:
        env_prefix = "postgresql_auth_"

    local: AuthLocalMethod = Field(
        default="trust",
        description="Default authentication method for local-socket connections.",
    )

    host: AuthHostMethod = Field(
        default="trust",
        description="Default authentication method for local TCP/IP connections.",
    )

    passfile: Path = Field(
        default=Path.home() / ".pgpass", description="Path to .pgpass file."
    )

    password_command: List[str] = Field(
        default=[], description="An optional command to retrieve PGPASSWORD from"
    )


@frozen
class InitdbSettings(BaseSettings):
    """Settings for initdb step of a PostgreSQL instance."""

    class Config:
        env_prefix = "postgresql_initdb_"

    locale: Optional[str] = Field(
        default="C", description="Instance locale as used by initdb."
    )

    encoding: Optional[str] = Field(
        default="UTF8", description="Instance encoding as used by initdb."
    )

    data_checksums: bool = Field(
        default=False, description="Use checksums on data pages."
    )


@frozen
class PostgreSQLSettings(BaseSettings):
    """Settings for PostgreSQL."""

    class Config:
        env_prefix = "postgresql_"

    bindir: Optional[str] = Field(
        default=bindir, description="Default PostgreSQL bindir, templated by version."
    )

    versions: Dict[str, PostgreSQLVersionSettings] = Field(
        default_factory=lambda: {}, description="Available PostgreSQL versions."
    )

    @root_validator
    def set_versions(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        bindir = values["bindir"]
        pgversions = values["versions"]
        if bindir is not None:
            for version in PostgreSQLVersion.__members__.values():
                if version not in pgversions:
                    pgversions[version] = PostgreSQLVersionSettings(
                        bindir=bindir.format(version=version)
                    )
        return values

    default_version: Optional[PostgreSQLVersion] = Field(
        default=None, description="Default PostgreSQL version to use, if unspecified."
    )

    root: DataPath = Field(
        default=DataPath("pgsql"),
        description="Root directory for all managed instances.",
    )

    initdb: InitdbSettings = InitdbSettings()

    auth: AuthSettings = AuthSettings()

    @frozen
    class SuRole(BaseSettings):
        name: str = "postgres"
        pgpass: bool = Field(
            default=False, description="Whether to store the password in .pgpass file."
        )

    surole: SuRole = Field(default=SuRole(), description="Instance super-user role.")

    replrole: str = Field(
        default="replication", description="Instance replication role."
    )

    backuprole: str = Field(
        default="backup", description="Instance role used to backup."
    )

    datadir: str = Field(
        default="data",
        description="Path segment from instance base directory to PGDATA directory.",
    )

    waldir: str = Field(
        default="wal",
        description="Path segment from instance base directory to WAL directory.",
    )

    pid_directory: RunPath = Field(
        default=RunPath("postgresql"),
        description="Path to directory where postgres process PID file will be written.",
    )

    socket_directory: RunPath = Field(
        default=RunPath("postgresql"),
        description="Path to directory where postgres unix socket will be written.",
    )

    def libpq_environ(
        self,
        ctx: "BaseContext",
        instance: "BaseInstance",
        *,
        base: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """Return a dict with libpq environment variables for authentication."""
        auth = self.auth
        if base is None:
            env = os.environ.copy()
        else:
            env = base.copy()
        env.setdefault("PGPASSFILE", str(self.auth.passfile))
        if auth.password_command and "PGPASSWORD" not in env:
            try:
                cmd = [c.format(instance=instance) for c in auth.password_command]
            except ValueError as e:
                raise exceptions.SettingsError(
                    f"failed to format auth.password_command: {e}"
                ) from None
            password = ctx.run(cmd, log_output=False, check=True).stdout.strip()
            if password:
                env["PGPASSWORD"] = password
        return env


@frozen
class PgBackRestSettings(PluginSettings):
    """Settings for pgBackRest."""

    class Config:
        env_prefix = "pgbackrest_"

    execpath: FilePath = Field(
        default=Path("/usr/bin/pgbackrest"),
        description="Path to the pbBackRest executable.",
    )

    configpath: ConfigPath = Field(
        default=ConfigPath(
            "pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf"
        ),
        description="Path to the config file.",
    )

    directory: DataPath = Field(
        default=DataPath("pgbackrest/{instance.version}-{instance.name}"),
        description="Path to the directory where backups are stored.",
    )

    logpath: DataPath = Field(
        default=DataPath("pgbackrest/{instance.version}-{instance.name}/logs"),
        description="Path where log files are stored.",
    )

    spoolpath: DataPath = Field(
        default=DataPath("pgbackrest/{instance.version}-{instance.name}/spool"),
        description="Spool path.",
    )

    lockpath: RunPath = Field(
        default=RunPath("pgbackrest/{instance.version}-{instance.name}/lock"),
        description="Path where lock files are stored.",
    )


@frozen
class PrometheusSettings(PluginSettings):
    """Settings for Prometheus postgres_exporter"""

    class Config:
        env_prefix = "prometheus_"

    execpath: FilePath = Field(description="Path to the postgres_exporter executable.")

    role: str = Field(
        default="prometheus",
        description="Name of the PostgreSQL role for Prometheus postgres_exporter.",
    )

    configpath: ConfigPath = Field(
        default=ConfigPath("prometheus/postgres_exporter-{name}.conf"),
        description="Path to the config file.",
    )

    queriespath: ConfigPath = Field(
        default=ConfigPath("prometheus/postgres_exporter_queries-{name}.yaml"),
        description="Path to the queries file.",
    )

    pid_file: RunPath = Field(
        default=RunPath("prometheus/{name}.pid"),
        description="Path to directory where postgres_exporter process PID file will be written.",
    )


@frozen
class PowaSettings(PluginSettings):
    """Settings for PoWA."""

    class Config:
        env_prefix = "powa_"

    dbname: str = Field(default="powa", description="Name of the PoWA database")

    role: str = Field(default="powa", description="Instance role used for PoWA.")


@frozen
class TemboardSettings(PluginSettings):
    """Settings for temBoard agent"""

    class Config:
        env_prefix = "temboard_"

    class Plugin(types.AutoStrEnum):
        activity = enum.auto()
        administration = enum.auto()
        dashboard = enum.auto()
        maintenance = enum.auto()
        monitoring = enum.auto()
        pgconf = enum.auto()
        statements = enum.auto()

    execpath: FilePath = Field(
        default=Path("/usr/bin/temboard-agent"),
        description="Path to the temboard-agent executable.",
    )

    role: str = Field(
        default="temboardagent",
        description="Name of the PostgreSQL roel for temBoard agent.",
    )

    configpath: ConfigPath = Field(
        default=ConfigPath("temboard-agent/temboard-agent-{name}.conf"),
        description="Path to the config file.",
    )

    pid_file: RunPath = Field(
        default=RunPath("temboard-agent/temboard-agent-{name}.pid"),
        description="Path to the PID file.",
    )

    users_path: ConfigPath = Field(
        default=ConfigPath("temboard-agent/users-{name}"),
        description="Path to the users/password file for the HTTP API.",
    )

    plugins: List[Plugin] = Field(
        default=[
            Plugin.monitoring,
            Plugin.dashboard,
            Plugin.activity,
        ],
        description="Plugins to load.",
    )

    ssl_cert_dir: ConfigPath = Field(
        default=ConfigPath("temboard-agent"),
        description="Path to directory where SSL certificate files will be written.",
    )

    home: DataPath = Field(
        default=DataPath("temboard-agent/{name}"),
        description="Path to agent home directory containing files used to store temporary data",
    )


@frozen
class SystemdSettings(BaseSettings):
    """Systemd settings."""

    class Config:
        env_prefix = "systemd_"

    unit_path: Path = Field(
        default=default_systemd_unit_path(os.getuid()),
        description="Base path where systemd units will be installed.",
    )

    user: bool = Field(
        default=True,
        description="Use the system manager of the calling user, by passing --user to systemctl calls.",
    )

    sudo: bool = Field(
        default=False,
        description="Run systemctl command with sudo; only applicable when 'user' is unset.",
    )

    @root_validator
    def __sudo_and_user(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values["user"] and values["sudo"]:
            raise ValueError("'user' mode cannot be used with 'sudo'")
        return values


def yaml_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """Load settings values 'settings.yaml' file if found in user or system
    config directory directory.
    """
    assert isinstance(settings, SiteSettings)
    fpath = settings.site_settings()
    if fpath is None:
        return {}
    with fpath.open() as f:
        settings = yaml.safe_load(f)
    if settings is None:
        return {}
    if not isinstance(settings, dict):
        raise exceptions.SettingsError(
            f"failed to load site settings from '{fpath}', expecting an object"
        )
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
    pgbackrest: Optional[PgBackRestSettings] = None
    powa: Optional[PowaSettings] = None
    prometheus: Optional[PrometheusSettings] = None
    temboard: Optional[TemboardSettings] = None
    systemd: SystemdSettings = SystemdSettings()

    service_manager: Optional[Literal["systemd"]] = None
    scheduler: Optional[Literal["systemd"]] = None

    prefix: Path = Field(
        default=default_prefix(os.getuid()),
        description="Path prefix for configuration and data files.",
    )

    run_prefix: Path = Field(
        default=default_run_prefix(os.getuid()),
        description="Path prefix for runtime socket, lockfiles and PID files.",
    )

    logpath: LogPath = Field(
        default=LogPath(),
        description="Directory where temporary log files from command executions will be stored",
        title="CLI log directory",
    )

    sysuser: Tuple[str, str] = Field(
        default_factory=default_sysuser,
        help=(
            "(username, groupname) of system user running PostgreSQL; "
            "mostly applicable when operating PostgreSQL with systemd in non-user mode"
        ),
    )

    @root_validator
    def __prefix_paths(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Prefix child settings fields with the global 'prefix'."""
        for key, child in values.items():
            if isinstance(child, PrefixedPath):
                values[key] = child.prefix(values[child.key])
            elif isinstance(child, BaseSettings):
                update = {}
                for fn, mf in child.__fields__.items():
                    # mf.types_ may be a typing.* class, which is not a type.
                    if isinstance(mf.type_, type) and issubclass(
                        mf.type_, PrefixedPath
                    ):
                        prefixed_path = getattr(child, fn)
                        update[fn] = prefixed_path.prefix(values[prefixed_path.key])
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


@frozen
class SiteSettings(Settings):
    """Settings loaded from site-sources.

    Load user or site settings from:
    - 'settings.yaml' if found in user or system configuration directory, and,
    - SETTINGS environment variable.
    """

    @staticmethod
    def site_settings() -> Optional[Path]:
        """Return path to 'settings.yaml' if found in site configuration
        directories.
        """
        for hdlr in (util.xdg_config, util.etc_config):
            fpath = hdlr("settings.yaml")
            if fpath is not None:
                return fpath
        return None

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


def plugins(settings: Settings) -> Iterator[Tuple[str, Optional[PluginSettings]]]:
    """Return an iterator of 'settings' fields and names for plugins."""
    for name, field in settings.__class__.__fields__.items():
        if lenient_issubclass(field.type_, PluginSettings):
            yield name, getattr(settings, field.name)
