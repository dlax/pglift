import subprocess
from typing import TYPE_CHECKING, Any, Sequence

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
