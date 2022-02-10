import enum
import json
import subprocess
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

import psycopg.errors
import yaml
from pgtoolkit import conf as pgconf
from pydantic import BaseModel, SecretStr
from typing_extensions import Protocol, TypedDict

if TYPE_CHECKING:
    CompletedProcess = subprocess.CompletedProcess[str]
else:
    CompletedProcess = subprocess.CompletedProcess


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


class Role(Protocol):
    name: str
    password: Optional[SecretStr]


class NoticeHandler(Protocol):
    def __call__(self, diag: psycopg.errors.Diagnostic) -> Any:
        ...


class StrEnum(str, enum.Enum):
    pass


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


class Manifest(BaseModel):
    """Base class for manifest data classes."""

    _cli_config: ClassVar[Dict[str, CLIConfig]] = {}
    _ansible_config: ClassVar[Dict[str, AnsibleConfig]] = {}

    class Config:
        extra = "forbid"

    _M = TypeVar("_M", bound="Manifest")

    @classmethod
    def parse_yaml(cls: Type[_M], stream: IO[str]) -> _M:
        """Parse from a YAML stream."""
        data = yaml.safe_load(stream)
        return cls.parse_obj(data)

    def yaml(self, **kwargs: Any) -> str:
        """Return a YAML serialization of this manifest."""
        data = json.loads(self.json(**kwargs))
        return yaml.dump(data, sort_keys=False)  # type: ignore[no-any-return]


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
