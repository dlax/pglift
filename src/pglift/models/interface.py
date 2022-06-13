import enum
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import psycopg.conninfo
from pgtoolkit import conf as pgconf
from pgtoolkit.ctl import Status
from pydantic import (
    BaseModel,
    ByteSize,
    DirectoryPath,
    Field,
    SecretStr,
    ValidationError,
    create_model,
    root_validator,
    validator,
)
from pydantic.error_wrappers import ErrorWrapper
from pydantic.utils import lenient_issubclass

from .. import settings
from .._compat import Final, Literal
from ..types import AnsibleConfig, AutoStrEnum, CLIConfig
from ..types import Extension as Extension
from ..types import Manifest, Port, ServiceManifest

if TYPE_CHECKING:
    from ..pm import PluginManager

default_port: Final = 5432


def validate_ports(model: BaseModel) -> None:
    """Walk fields of 'model', checking those with type Port if their value is
    available.
    """

    def _validate(
        model: BaseModel, *, loc: Tuple[str, ...] = ()
    ) -> Iterator[ErrorWrapper]:
        cls = model.__class__
        for name, field in cls.__fields__.items():
            value = getattr(model, name)
            if value is None:
                continue
            ftype = field.outer_type_
            if lenient_issubclass(ftype, BaseModel):
                yield from _validate(value, loc=loc + (name,))
            elif lenient_issubclass(ftype, Port):
                assert isinstance(value, Port)
                if not value.available():
                    yield ErrorWrapper(
                        ValueError(f"port {value} already in use"), loc + (name,)
                    )

    errors = list(_validate(model))
    if errors:
        raise ValidationError(errors, model.__class__)


class InstanceState(AutoStrEnum):
    """Instance state."""

    stopped = enum.auto()
    """stopped"""

    started = enum.auto()
    """started"""

    absent = enum.auto()
    """absent"""

    restarted = enum.auto()
    """restarted"""

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


class InstanceListItem(Manifest):

    name: str
    version: str
    port: int
    path: DirectoryPath
    status: str


class BaseInstance(Manifest):
    """PostgreSQL instance suitable for lookup"""

    name: str = Field(readOnly=True, description=("Instance name."))
    version: Optional[settings.PostgreSQLVersion] = Field(
        default=None, description="PostgreSQL version.", readOnly=True
    )

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
        >>> Instance(name='with/slash')
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        name
          instance name must not contain slashes (type=value_error)
        """
        # Avoid dash as this will break systemd instance unit.
        if "-" in v:
            raise ValueError("instance name must not contain dashes")
        # Likewise, slash messes up with file paths.
        if "/" in v:
            raise ValueError("instance name must not contain slashes")
        return v


class Role(Manifest):
    """PostgreSQL role"""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {
        "in_roles": {"name": "in_role"},
        "state": {"hide": True},
    }

    name: str = Field(readOnly=True, description=("Role name."))
    password: Optional[SecretStr] = Field(default=None, description="Role password.")
    inherit: bool = Field(
        default=True,
        description="Let the role inherits the privileges of the roles its is a member of.",
    )
    login: bool = Field(default=False, description="Allow the role to log in.")
    superuser: bool = Field(default=False, description="Superuser role.")
    replication: bool = Field(default=False, description="Replication role.")
    connection_limit: Optional[int] = Field(
        description="How many concurrent connections the role can make.",
    )
    validity: Optional[datetime] = Field(
        description="Sets a date and time after which the role's password is no longer valid."
    )
    in_roles: List[str] = Field(
        default_factory=list,
        description="List of roles to which the new role will be added as a new member.",
    )
    pgpass: bool = Field(
        default=False, description="Add an entry in password file for this role."
    )
    state: PresenceState = Field(
        default=PresenceState.present, description=("Role state.")
    )


class Database(Manifest):
    """PostgreSQL database"""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {
        "settings": {"hide": True},
        "state": {"hide": True},
        "extensions": {"name": "extension"},
    }
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {
        "settings": {"spec": {"type": "dict", "required": False}},
    }
    name: str = Field(readOnly=True, description=("Database name."))
    owner: Optional[str] = Field(
        description="The role name of the user who will own the database."
    )
    settings: Optional[Dict[str, Optional[pgconf.Value]]] = Field(
        default=None,
        description=(
            "Session defaults for run-time configuration variables for the database. "
            "Upon update, an empty (dict) value would reset all settings."
        ),
    )
    extensions: List[Extension] = Field(
        default_factory=list,
        description="List of extensions to create in the database.",
    )
    state: PresenceState = Field(
        default=PresenceState.present, description=("Database state.")
    )


class Instance(BaseInstance):
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
        "pending_restart": {"hide": True},
        "restart_on_changes": {"hide": True},
        "ssl": {"hide": True},
        "configuration": {"hide": True},
        "extensions": {"name": "extension"},
        "roles": {"hide": True},
        "databases": {"hide": True},
    }
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {
        "ssl": {
            "spec": {
                "type": "bool",
                "required": False,
                "default": False,
                "description": [
                    "Enable SSL",
                    "If True, enable SSL and generated a self-signed certificate",
                ],
            }
        },
        "configuration": {"spec": {"type": "dict", "required": False}},
        "pending_restart": {"hide": True},
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
        _cli_config: ClassVar[Dict[str, CLIConfig]] = {
            "status": {"hide": True},
            "replication_lag": {"hide": True},
        }
        _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {
            "replication_lag": {"hide": True}
        }

        @enum.unique
        class State(AutoStrEnum):
            """Instance standby status"""

            demoted = enum.auto()
            promoted = enum.auto()

        for_: str = Field(
            alias="for",
            description="DSN of primary for streaming replication.",
        )
        password: Optional[SecretStr] = Field(
            default=None, description="Password for the replication user."
        )
        status: State = Field(
            default=State.demoted, description=("Instance standby state.")
        )
        slot: Optional[str] = Field(description="Replication slot name.")
        replication_lag: Optional[Decimal] = Field(
            default=None, description="Replication lag.", readOnly=True
        )

        @validator("for_")
        def __validate_for_(cls, value: str) -> str:
            """Validate 'for' field.

            >>> Instance.Standby.parse_obj({"for": "host=localhost"})
            Standby(for_='host=localhost', password=None, status=<State.demoted: 'demoted'>, slot=None, replication_lag=None)
            >>> Instance.Standby.parse_obj({"for": "hello"})
            Traceback (most recent call last):
                ...
            pydantic.error_wrappers.ValidationError: 1 validation error for Standby
            for
              missing "=" after "hello" in connection info string
             (type=value_error)
            >>> Instance.Standby.parse_obj({"for": "host=localhost password=xx"})
            Traceback (most recent call last):
                ...
            pydantic.error_wrappers.ValidationError: 1 validation error for Standby
            for
              connection string must not contain a password (type=value_error)
            """
            try:
                conninfo = psycopg.conninfo.conninfo_to_dict(value)
            except psycopg.ProgrammingError as e:
                raise ValueError(str(e))
            if "password" in conninfo:
                raise ValueError("connection string must not contain a password")
            return value

        @property
        def primary_conninfo(self) -> str:
            """Connection string to the primary.

            >>> s = Instance.Standby.parse_obj({"for": "host=primary port=5444", "password": "qwerty"})
            >>> s.primary_conninfo
            'host=primary port=5444 password=qwerty'
            """
            kw = {}
            if self.password:
                kw["password"] = self.password.get_secret_value()
            return psycopg.conninfo.make_conninfo(self.for_, **kw)

    port: Port = Field(
        default=Port(default_port),
        description="TCP port the postgresql instance will be listening to.",
    )
    ssl: Union[bool, Tuple[Path, Path]] = Field(
        default=False,
        title="SSL",
        description=(
            "Enable SSL. "
            "If True, enable SSL and generated a self-signed certificate. "
            "If a list of [cert_file, key_file], enable SSL and use given certificate."
        ),
    )
    configuration: Dict[str, Any] = Field(
        default_factory=dict, description=("Settings for the PostgreSQL instance.")
    )
    surole_password: Optional[SecretStr] = Field(
        default=None, description="Super-user role password.", readOnly=True
    )
    replrole_password: Optional[SecretStr] = Field(
        default=None, description="Replication role password.", readOnly=True
    )
    data_checksums: Optional[bool] = Field(
        default=None,
        description=(
            "Enable or disable data checksums. "
            "If unspecified, fall back to site settings choice."
        ),
    )
    locale: Optional[str] = Field(
        default=None, description="Default locale.", readOnly=True
    )
    encoding: Optional[str] = Field(
        default=None,
        description="Character encoding of the PostgreSQL instance.",
        readOnly=True,
    )

    class Auth(BaseModel):
        local: Optional[settings.AuthLocalMethod] = Field(
            default=None,
            description="Authentication method for local-socket connections",
            readOnly=True,
        )
        host: Optional[settings.AuthHostMethod] = Field(
            default=None,
            description="Authentication method for local TCP/IP connections",
            readOnly=True,
        )

    auth: Optional[Auth] = Field(default=None, exclude=True, writeOnly=True)

    extensions: List[Extension] = Field(
        default_factory=list,
        description="List of extensions to install in the instance.",
    )

    standby: Optional[Standby] = None

    state: InstanceState = Field(
        default=InstanceState.started,
        description="Runtime state.",
    )
    databases: List["Database"] = Field(
        default_factory=list,
        description="Databases",
        exclude=True,
        writeOnly=True,
    )
    roles: List["Role"] = Field(
        default_factory=list,
        description="Roles",
        exclude=True,
        writeOnly=True,
    )

    pending_restart: bool = Field(
        default=False,
        description="Whether the instance needs a restart to account for configuration changes.",
        readOnly=True,
    )
    restart_on_changes: bool = Field(
        default=False,
        description="Whether or not to automatically restart the instance to account for configuration changes.",
        exclude=True,
        writeOnly=True,
    )

    @root_validator
    def __validate_port_(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that 'port' field and configuration['port'] are consistent.

        >>> Instance(name="i")
        Instance(name='i', ...)
        >>> Instance(name="i", port=123, configuration={"port": 123})
        Instance(name='i', ...)
        >>> Instance(name="i", port=321, configuration={"port": 123})
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        __root__
          'port' field and configuration['port'] mistmatch (type=value_error)
        """
        try:
            port = values["port"]
        except KeyError:
            return values
        try:
            config_port = values.get("configuration", {})["port"]
        except KeyError:
            return values
        if config_port != port:
            raise ValueError("'port' field and configuration['port'] mistmatch")
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
    size: ByteSize
    repo_size: ByteSize
    date_start: datetime
    date_stop: datetime
    type: Literal["incr", "diff", "full"]
    databases: List[str]


class Tablespace(BaseModel):
    name: str
    location: str
    size: ByteSize


class DetailedDatabase(Manifest):
    """PostgreSQL database (with details)"""

    name: str
    owner: str
    encoding: str
    collation: str
    ctype: str
    acls: List[str]
    size: ByteSize
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


class DefaultPrivilege(Manifest):
    """Default access privilege"""

    database: str
    schema_: str = Field(alias="schema")
    object_type: str
    role: str
    privileges: List[str]

    @validator("privileges")
    def __sort_privileges_(cls, value: List[str]) -> List[str]:
        return sorted(value)


class Privilege(DefaultPrivilege):
    """Access privilege"""

    object_name: str
    column_privileges: Dict[str, List[str]]

    @validator("column_privileges")
    def __sort_column_privileges_(
        cls, value: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        for v in value.values():
            v.sort()
        return value


class PGSetting(Manifest):
    """A column from pg_settings view."""

    _query: ClassVar[
        str
    ] = "SELECT name, setting, context, pending_restart FROM pg_settings"

    name: str
    setting: str
    context: str
    pending_restart: bool
