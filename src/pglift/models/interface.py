import enum
import json
from datetime import datetime
from pathlib import Path
from typing import IO, Any, ClassVar, Dict, List, Optional, Tuple, Type, TypeVar, Union

import psycopg
import psycopg.conninfo
import yaml
from pgtoolkit.ctl import Status
from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    SecretStr,
    root_validator,
    validator,
)
from typing_extensions import Literal, TypedDict

from .. import prometheus_default_port, settings
from ..types import AnsibleArgSpec, AutoStrEnum


class CLIConfig(TypedDict, total=False):
    """Configuration for CLI generation of a manifest field."""

    name: str
    hide: bool
    choices: List[str]


class AnsibleConfig(TypedDict, total=False):
    hide: bool
    choices: List[str]
    spec: AnsibleArgSpec


class InstanceState(AutoStrEnum):
    """Instance state."""

    stopped = enum.auto()
    """stopped"""

    started = enum.auto()
    """started"""

    absent = enum.auto()
    """absent"""

    @classmethod
    def from_pg_status(cls, status: Status) -> "InstanceState":
        """Instance state from PostgreSQL status.

        >>> InstanceState.from_pg_status(Status.running)
        <InstanceState.started: 'started'>
        >>> InstanceState.from_pg_status(Status.not_running)
        <InstanceState.stopped: 'stopped'>
        >>> InstanceState.from_pg_status(Status.unspecified_datadir)
        <InstanceState.absent: 'absent'>
        """
        return cls(
            {
                status.running: cls.started,
                status.not_running: cls.stopped,
                status.unspecified_datadir: cls.absent,
            }[status]
        )


class PresenceState(AutoStrEnum):
    """Should the object be present or absent?"""

    present = enum.auto()
    absent = enum.auto()


class InstanceListItem(BaseModel):

    name: str
    version: str
    port: int
    path: DirectoryPath
    status: str


T = TypeVar("T", bound=BaseModel)


class Manifest(BaseModel):
    """Base class for manifest data classes."""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {}
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {}

    class Config:
        extra = "forbid"

    @classmethod
    def parse_yaml(cls: Type[T], stream: IO[str]) -> T:
        """Parse from a YAML stream."""
        data = yaml.safe_load(stream)
        return cls.parse_obj(data)

    def yaml(self, **kwargs: Any) -> str:
        """Return a YAML serialization of this manifest."""
        data = json.loads(self.json(**kwargs))
        return yaml.dump(data, sort_keys=False)  # type: ignore[no-any-return]


class Instance(Manifest):
    """PostgreSQL instance"""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {
        "status": {"hide": True},
        "state": {
            "choices": [InstanceState.started.value, InstanceState.stopped.value]
        },
        "ssl": {"hide": True},
        "configuration": {"hide": True},
    }
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {
        "ssl": {"spec": {"type": "bool", "required": False, "default": False}},
        "configuration": {"spec": {"type": "dict", "required": False}},
    }

    class Standby(BaseModel):
        _cli_config: ClassVar[Dict[str, CLIConfig]] = {"status": {"hide": True}}

        @enum.unique
        class State(AutoStrEnum):
            """Instance standby status"""

            demoted = enum.auto()
            promoted = enum.auto()

        for_: str = Field(
            alias="for",
            description="DSN of primary for streaming replication",
        )
        status: State = Field(
            default=State.demoted,
        )
        slot: Optional[str] = Field(description="replication slot name")

    class Prometheus(BaseModel):
        port: int = Field(
            default=prometheus_default_port,
            description="TCP port for the web interface and telemetry of Prometheus",
        )

    name: str
    version: Optional[str] = Field(default=None, description="PostgreSQL version")
    port: Optional[int] = Field(
        default=None,
        description="TCP port the postgresql instance will be listening to",
    )
    state: InstanceState = Field(
        default=InstanceState.started,
        description="Runtime state",
    )
    ssl: Union[bool, Tuple[Path, Path]] = Field(default=False, title="SSL")
    configuration: Dict[str, Any] = Field(default_factory=dict)
    surole_password: Optional[SecretStr] = Field(
        default=None,
        description="super-user role password",
    )
    replrole_password: Optional[SecretStr] = Field(
        default=None,
        description="replication role password",
    )
    data_checksums: Optional[bool] = Field(
        default=None,
        description=(
            "Enable or disable data checksums. "
            "If None, fall back to site settings initdb.data_checksums."
        ),
    )

    standby: Optional[Standby] = None

    prometheus: Optional[Prometheus] = Prometheus()

    @validator("name")
    def __validate_name_(cls, v: str) -> str:
        """Validate 'name' field.

        >>> Instance(name='without_dash')  # doctest: +ELLIPSIS
        Instance(name='without_dash', ...)
        >>> Instance(name='with-dash')
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        name
          instance name must not contain dashes (type=value_error)
        """
        # Avoid dash as this will break systemd instance unit.
        if "-" in v:
            raise ValueError("instance name must not contain dashes")
        return v

    @validator("version")
    def __validate_version_(cls, v: Optional[str]) -> Optional[str]:
        """Validate 'version' field.

        >>> Instance(name="x", version=None).version
        >>> Instance(name="x", version="13").version
        '13'
        >>> Instance(name="x", version="9")
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        version
          unsupported PostgreSQL version: 9 (type=value_error)
        """
        if v is None:
            return None
        if v not in settings.POSTGRESQL_SUPPORTED_VERSIONS:
            raise ValueError(f"unsupported PostgreSQL version: {v}")
        return v

    @root_validator
    def __port_not_in_configuration_(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that 'configuration' field has no 'port' key.

        >>> Instance(name="i")
        Instance(name='i', ...)
        >>> Instance(name="i", configuration={"port": 123})
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        __root__
          port should not be specified in configuration field (type=value_error)
        """
        if "port" in values.get("configuration", {}):
            raise ValueError("port should not be specified in configuration field")
        return values


class InstanceBackup(Manifest):
    label: str
    size: float
    repo_size: float
    datetime: datetime
    type: Literal["incr", "diff", "full"]
    databases: str


class PostgresExporter(Manifest):
    """Prometheus postgres_exporter service."""

    class State(AutoStrEnum):
        """Runtime state"""

        started = enum.auto()
        stopped = enum.auto()
        absent = enum.auto()

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {
        "state": {"choices": [State.started.value, State.stopped.value]},
    }

    name: str = Field(description="locally unique identifier of the service")
    dsn: str = Field(description="connection string of target instance")
    password: Optional[SecretStr] = Field(description="connection password")
    port: int = Field(description="TCP port for the web interface and telemetry")
    state: State = Field(default=State.started, description="runtime state")

    @validator("name")
    def __validate_name_(cls, v: str) -> str:
        """Validate 'name' field.

        >>> PostgresExporter(name='without-slash', dsn="", port=12)  # doctest: +ELLIPSIS
        PostgresExporter(name='without-slash', ...)
        >>> PostgresExporter(name='with/slash', dsn="", port=12)
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for PostgresExporter
        name
          must not contain slashes (type=value_error)
        """
        # Avoid slash as this will break file paths during settings templating
        # (configpath, etc.)
        if "/" in v:
            raise ValueError("must not contain slashes")
        return v

    @validator("dsn")
    def __validate_dsn_(cls, value: str) -> str:
        try:
            psycopg.conninfo.conninfo_to_dict(value)
        except psycopg.ProgrammingError as e:
            raise ValueError(str(e)) from e
        return value


class Role(Manifest):
    """PostgreSQL role"""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {
        "in_roles": {"name": "in-role"},
        "state": {"hide": True},
    }

    name: str
    password: Optional[SecretStr] = Field(default=None, description="role password")
    pgpass: bool = Field(
        default=False, description="add an entry in password file for this role"
    )
    inherit: bool = Field(
        default=True,
        description="let the role inherits the privileges of the roles its is a member of",
    )
    login: bool = Field(default=False, description="allow the role to log in")
    superuser: bool = Field(default=False, description="superuser role")
    replication: bool = Field(default=False, description="replication role")
    connection_limit: Optional[int] = Field(
        description="how many concurrent connections the role can make",
    )
    validity: Optional[datetime] = Field(
        description="sets a date and time after which the role's password is no longer valid"
    )
    in_roles: List[str] = Field(
        default_factory=list,
        description="list of roles to which the new role will be added as a new member",
    )
    state: PresenceState = Field(default=PresenceState.present)


class Database(Manifest):
    """PostgreSQL database"""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {"state": {"hide": True}}

    name: str
    owner: Optional[str] = Field(
        description="the role name of the user who will own the new database"
    )
    state: PresenceState = Field(default=PresenceState.present)


class Tablespace(BaseModel):
    name: str
    location: str
    size: int


class DetailedDatabase(Manifest):
    """PostgreSQL database (with details)"""

    name: str
    owner: str
    encoding: str
    collation: str
    ctype: str
    acls: Optional[List[str]]
    size: int
    description: Optional[str]
    tablespace: Tablespace

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        tablespace = kwargs["tablespace"]
        if not isinstance(tablespace, Tablespace):
            assert isinstance(tablespace, str)
            try:
                kwargs["tablespace"] = Tablespace(
                    name=tablespace,
                    location=kwargs.pop("tablespace_location"),
                    size=kwargs.pop("tablespace_size"),
                )
            except KeyError as exc:
                raise TypeError(f"missing {exc} argument when 'tablespace' is a string")
        super().__init__(**kwargs)


class Privilege(Manifest):
    """Access privilege"""

    database: str
    schema_: str = Field(alias="schema")
    role: str
    object_type: str
    privileges: List[str]


class PGSetting(Manifest):
    """A column from pg_settings view."""

    _query: ClassVar[
        str
    ] = "SELECT name, setting, context, pending_restart FROM pg_settings"

    name: str
    setting: str
    context: str
    pending_restart: bool


def instance_surole(settings: settings.Settings, instance: Instance) -> Role:
    surole_settings = settings.postgresql.surole
    if instance.surole_password:
        return Role(
            name=surole_settings.name,
            password=instance.surole_password,
            pgpass=surole_settings.pgpass,
        )
    else:
        return Role(name=surole_settings.name)


def instance_replrole(settings: settings.Settings, instance: Instance) -> Role:
    replrole_settings = settings.postgresql.replrole
    if instance.replrole_password:
        return Role(
            name=replrole_settings,
            password=instance.replrole_password,
            login=True,
            replication=True,
        )
    else:
        return Role(
            name=replrole_settings,
            login=True,
            replication=True,
        )
