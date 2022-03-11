import logging
from abc import ABC, abstractmethod
from typing import Any, Optional, Sequence

from . import cmd, plugin_manager
from ._compat import shlex_join
from .settings import Settings
from .types import CompletedProcess

logger = logging.getLogger(__name__)


class BaseContext(ABC):
    """Base class for execution context."""

    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings
        self.pm = plugin_manager(settings)
        self.hook = self.pm.hook

    @abstractmethod
    def run(
        self,
        args: Sequence[str],
        *,
        log_command: bool = True,
        check: bool = False,
        **kwargs: Any,
    ) -> CompletedProcess:
        """Execute a system command using chosen implementation."""
        ...

    def confirm(self, message: str, default: bool) -> bool:
        """Possible ask for confirmation of an action before running.

        Interactive implementations should prompt for confirmation with
        'message' and use the 'default' value as default. Non-interactive
        implementations (this one), will always return the 'default' value.
        """
        return default

    def prompt(self, message: str, hide_input: bool = False) -> Optional[str]:
        """Possible ask for user input.

        Interactive implementation should prompt for input with 'message' and
        return a string value. Non-Interactive implementations (this one), will
        always return None.
        """
        return None


class Context(BaseContext):
    """Default execution context."""

    def run(
        self, args: Sequence[str], log_command: bool = True, **kwargs: Any
    ) -> CompletedProcess:
        """Execute a system command with :func:`pglift.cmd.run`."""
        if log_command:
            logger.debug(shlex_join(args))
        return cmd.run(args, logger=logger, **kwargs)
