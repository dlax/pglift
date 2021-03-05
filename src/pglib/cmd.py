import subprocess
from typing import TYPE_CHECKING, Any, Optional, Sequence

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
        cwd: Optional[str] = None,
    ) -> CompletedProcess:
        ...


def run(args: Sequence[str], **kwargs: Any) -> CompletedProcess:
    return subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        **kwargs,
    )
