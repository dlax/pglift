from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Sequence, Union

from pgtoolkit import ctl
from pluggy import PluginManager

from . import cmd
from .settings import SETTINGS, Settings
from .types import CompletedProcess


class BaseContext(ABC):
    """Base class for execution context."""

    def __init__(
        self,
        *,
        plugin_manager: PluginManager,
        settings: Settings = SETTINGS,
        pg_bindir: Optional[Union[str, Path]] = None,
    ) -> None:
        self.settings = settings
        self.pg_ctl = ctl.PGCtl(pg_bindir, run_command=self.run)
        self.pm = plugin_manager

    @abstractmethod
    def run(
        self,
        args: Sequence[str],
        *,
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
