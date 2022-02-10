import enum
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pgtoolkit import conf as pgconf
from pgtoolkit.ctl import Status
from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    SecretStr,
    create_model,
    root_validator,
    validator,
)
from typing_extensions import Literal

from .. import settings
from ..types import AnsibleConfig, AutoStrEnum, CLIConfig, Manifest, ServiceManifest

if TYPE_CHECKING:
    from ..pm import PluginManager


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


class Instance(Manifest):
    """PostgreSQL instance"""

    class Config(Manifest.Config):
        # Allow extra fields to permit plugins to populate an object with
        # their specific data, following (hopefully) what's defined by
        # the "composite" model (see composite()).
        extra = "allow"

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

    _T = TypeVar("_T", bound="Instance")

    @classmethod
    def composite(cls: Type[_T], pm: "PluginManager") -> Type[_T]:
        """Create a model class, based on this one, with extra fields based on
        interface models for satellite components defined in plugins.
        """
        fields = {}
        for m in pm.hook.interface_model():
            sname = m.__service__
            if sname in fields:
                raise ValueError(f"duplicated '{sname}' service")
            fields[sname] = Optional[m], Field(default_factory=m)
        # XXX Spurious 'type: ignore' below.
        m = create_model(cls.__name__, __base__=cls, __module__=__name__, **fields)  # type: ignore[call-overload]
        # pydantic.create_model() uses type(), so this will confuse mypy which
        # cannot handle dynamic base class; hence the 'type: ignore'.
        return m  # type: ignore[no-any-return]

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

    _S = TypeVar("_S", bound=ServiceManifest)

    def service(self, stype: Type[_S]) -> Optional[_S]:
        """Return satellite service manifest attached to this instance.

        :raises ValueError: if not found.
        """
        fname = stype.__service__
        try:
            s = getattr(self, fname)
        except AttributeError:
            raise ValueError(fname)
        if s is None:
            return None
        assert isinstance(
            s, stype
        ), f"expecting field {fname} to have type {stype} (got {type(s)})"
        return s

    def surole(self, settings: settings.Settings) -> "Role":
        s = settings.postgresql.surole
        if self.surole_password:
            return Role(
                name=s.name,
                password=self.surole_password,
                pgpass=s.pgpass,
            )
        else:
            return Role(name=s.name)

    def replrole(self, settings: settings.Settings) -> "Role":
        name = settings.postgresql.replrole
        if self.replrole_password:
            return Role(
                name=name,
                password=self.replrole_password,
                login=True,
                replication=True,
            )
        else:
            return Role(
                name=name,
                login=True,
                replication=True,
            )


class InstanceBackup(Manifest):
    label: str
    size: float
    repo_size: float
    datetime: datetime
    type: Literal["incr", "diff", "full"]
    databases: str


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

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {
        "settings": {"hide": True},
        "state": {"hide": True},
    }
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {
        "settings": {"spec": {"type": "dict", "required": False}},
    }

    name: str
    owner: Optional[str] = Field(
        description="the role name of the user who will own the new database"
    )
    state: PresenceState = Field(default=PresenceState.present)
    settings: Optional[Dict[str, Optional[pgconf.Value]]] = Field(
        default=None,
        description="Session defaults for a run-time configuration variables for the database",
    )


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
