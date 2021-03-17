from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Sequence, Union

from pgtoolkit import ctl

from . import cmd
from .types import CompletedProcess


class BaseContext(ABC):
    """Base class for execution context."""

    def __init__(self, *, pg_bindir: Optional[Union[str, Path]] = None) -> None:
        self.pg_ctl = ctl.PGCtl(pg_bindir, run_command=self.run)

    @abstractmethod
    def run(
        self,
        args: Sequence[str],
        *,
        capture_output: bool = False,
        check: bool = False,
        **kwargs: Any,
    ) -> CompletedProcess:
        """Execute a system command using chosen implementation."""
        ...


class Context(BaseContext):
    """Default execution context."""

    @staticmethod
    def run(args: Sequence[str], **kwargs: Any) -> CompletedProcess:
        """Execute a system command with :func:`pglib.cmd.run`."""
        return cmd.run(args, **kwargs)
