import functools
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Sequence

from pgtoolkit import ctl

from . import cmd, exceptions, plugin_manager, util
from ._compat import shlex_join
from .settings import POSTGRESQL_SUPPORTED_VERSIONS, Settings
from .types import CompletedProcess

logger = logging.getLogger(__name__)


class BaseContext(ABC):
    """Base class for execution context."""

    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings
        self.pm = plugin_manager(settings)
        self.hook = self.pm.hook

    @functools.lru_cache(maxsize=len(POSTGRESQL_SUPPORTED_VERSIONS) + 1)
    def pg_ctl(self, version: Optional[str]) -> ctl.PGCtl:
        pg_bindir = None
        version = version or self.settings.postgresql.default_version
        if version is not None:
            pg_bindir = self.settings.postgresql.versions[version].bindir
        try:
            pg_ctl = ctl.PGCtl(pg_bindir, run_command=self.run)
        except EnvironmentError as e:
            raise exceptions.SystemError(str(e)) from e
        if version is not None:
            installed_version = util.short_version(pg_ctl.version)
            if installed_version != version:
                raise exceptions.SystemError(
                    f"PostgreSQL version from {pg_bindir} mismatches with declared value: "
                    f"{installed_version} != {version}"
                )
        return pg_ctl

    def libpq_environ(self, *, base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Return a dict with libpq environment variables for authentication."""
        auth = self.settings.postgresql.auth
        if base is None:
            env = os.environ.copy()
        else:
            env = base.copy()
        env.setdefault("PGPASSFILE", str(auth.passfile))
        if auth.password_command and "PGPASSWORD" not in env:
            password = self.run([auth.password_command], check=True).stdout.strip()
            if password:
                env["PGPASSWORD"] = password
        return env

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


class Context(BaseContext):
    """Default execution context."""

    def run(
        self, args: Sequence[str], log_command: bool = True, **kwargs: Any
    ) -> CompletedProcess:
        """Execute a system command with :func:`pglift.cmd.run`."""
        if log_command:
            logger.debug(shlex_join(args))
        return cmd.run(args, logger=logger, **kwargs)
