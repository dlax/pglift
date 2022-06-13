import enum
import json
import socket
import subprocess
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import psycopg.errors
import yaml
from pgtoolkit import conf as pgconf
from pydantic import BaseModel, SecretStr

from ._compat import Protocol, TypedDict

if TYPE_CHECKING:
    CompletedProcess = subprocess.CompletedProcess[str]
else:
    CompletedProcess = subprocess.CompletedProcess


class StrEnum(str, enum.Enum):
    def __str__(self) -> str:
        assert isinstance(self.value, str)
        return self.value


@enum.unique
class AutoStrEnum(StrEnum):
    """Enum base class with automatic values set to member name.

    >>> class State(AutoStrEnum):
    ...     running = enum.auto()
    ...     stopped = enum.auto()
    >>> State.running
    <State.running: 'running'>
    >>> State.stopped
    <State.stopped: 'stopped'>
    """

    def _generate_next_value_(name, *args: Any) -> str:  # type: ignore[override]
        return name


class CommandRunner(Protocol):
    def __call__(
        self,
        args: Sequence[str],
        *,
        check: bool = False,
        **kwargs: Any,
    ) -> CompletedProcess:
        ...


ConfigChanges = Dict[str, Tuple[Optional[pgconf.Value], Optional[pgconf.Value]]]


class Extension(AutoStrEnum):
    """A PostgreSQL instance-level extension"""

    btree_gist = enum.auto()
    passwordcheck = enum.auto()
    pg_qualstats = enum.auto()
    pg_stat_kcache = enum.auto()
    pg_stat_statements = enum.auto()
    powa = enum.auto()
    unaccent = enum.auto()


class BackupType(AutoStrEnum):
    """Backup type."""

    full = enum.auto()
    """full backup"""
    incr = enum.auto()
    """incremental backup"""
    diff = enum.auto()
    """differential backup"""

    @classmethod
    def default(cls) -> "BackupType":
        return cls.incr


class Role(Protocol):
    name: str
    password: Optional[SecretStr]


class NoticeHandler(Protocol):
    def __call__(self, diag: psycopg.errors.Diagnostic) -> Any:
        ...


class AnsibleArgSpec(TypedDict, total=False):
    required: bool
    type: str
    default: Any
    choices: List[str]
    description: List[str]
    no_log: bool


class CLIConfig(TypedDict, total=False):
    """Configuration for CLI generation of a manifest field."""

    name: str
    hide: bool
    choices: List[str]


class AnsibleConfig(TypedDict, total=False):
    hide: bool
    choices: List[str]
    spec: AnsibleArgSpec


class Port(int):
    """Port field type."""

    P = TypeVar("P", bound="Port")

    @classmethod
    def __get_validators__(cls: Type[P]) -> Iterator[Callable[..., P]]:
        yield cls.validate

    @classmethod
    def validate(cls: Type[P], value: Any) -> P:
        return cls(value)

    def available(self) -> bool:
        """Return True if this port is free to use."""
        with socket.socket() as s:
            try:
                s.bind(("", self))
            except socket.error:
                return False
        return True


class Manifest(BaseModel):
    """Base class for manifest data classes."""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {}
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {}

    class Config:
        allow_mutation = False
        extra = "forbid"
        validate_always = True
        validate_assignment = True

    _M = TypeVar("_M", bound="Manifest")

    @classmethod
    def parse_yaml(cls: Type[_M], value: Union[str, IO[str]]) -> _M:
        """Parse from a YAML stream."""
        data = yaml.safe_load(value)
        return cls.parse_obj(data)

    def yaml(self, **kwargs: Any) -> str:
        """Return a YAML serialization of this manifest."""
        data = json.loads(self.json(by_alias=True, **kwargs))
        return yaml.dump(data, sort_keys=False, explicit_start=True)  # type: ignore[no-any-return]

    def _copy_validate(self: _M, update: Dict[str, Any]) -> _M:
        """Like .copy(), but with validation (and default value setting).

        (Internal method, mostly useful for test purpose.)
        """
        return self.__class__.validate(self.copy(update=update).dict())


class ServiceManifest(Manifest):
    __service__: ClassVar[str]

    def __init_subclass__(cls, *, service_name: str, **kwargs: Any) -> None:
        """Set a __name__ to subclasses.

        >>> class MyS(ServiceManifest, service_name="my"):
        ...     x: str
        >>> s = MyS(x=1)
        >>> s.__class__.__service__
        'my'
        """
        super().__init_subclass__(**kwargs)
        cls.__service__ = service_name
