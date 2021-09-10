import enum
import subprocess
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple

from pgtoolkit import conf as pgconf
from pydantic import SecretStr
from typing_extensions import Protocol

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
    pgpass: bool


class Logger(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...


class NoticeHandler(Protocol):
    def append(self, notice: str) -> None:
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
