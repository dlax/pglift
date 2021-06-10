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
