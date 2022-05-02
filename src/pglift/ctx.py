import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Sequence

from . import cmd, plugin_manager, util
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

    @staticmethod
    def site_config(*parts: str) -> Optional[Path]:
        """Lookup for a configuration file path.

        $XDG_CONFIG_HOME/pglift, /etc/pglift and then distribution data
        directory are inspected in this order.
        """
        for hdlr in (util.etc_config, util.xdg_config, util.dist_config):
            config = hdlr(*parts)
            if config:
                return config
        return None


class Context(BaseContext):
    """Default execution context."""

    def run(
        self,
        args: Sequence[str],
        log_command: bool = True,
        log_output: bool = True,
        **kwargs: Any,
    ) -> CompletedProcess:
        """Execute a system command with :func:`pglift.cmd.run`."""
        if log_command:
            logger.debug(shlex_join(args))
        stdout_logger = logger if log_output else None
        return cmd.run(
            args, stdout_logger=stdout_logger, stderr_logger=logger, **kwargs
        )
