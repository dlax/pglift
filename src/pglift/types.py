import enum
import subprocess
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

import psycopg.errors
from pgtoolkit import conf as pgconf
from pydantic import SecretStr
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
