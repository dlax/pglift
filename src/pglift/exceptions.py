import abc
import subprocess
from typing import Optional, Sequence


class Error(Exception, metaclass=abc.ABCMeta):
    """Base class for operational error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class NotFound(Error, metaclass=abc.ABCMeta):
    """Base class for errors when an object with `name` is not found."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(name)

    @abc.abstractproperty
    def object_type(self) -> str:
        """Type of object that's not found."""
        raise NotImplementedError

    def __str__(self) -> str:
        return f"{self.object_type} '{self.name}' not found"


class InstanceNotFound(NotFound):
    """PostgreSQL instance not found or mis-configured."""

    object_type = "instance"


class RoleNotFound(NotFound):
    """PostgreSQL role not found."""

    object_type = "role"


class DatabaseNotFound(NotFound):
    """PostgreSQL database not found."""

    object_type = "database"


class CommandError(subprocess.CalledProcessError, Error):
    """Execution of a command, in a subprocess, failed."""

    def __init__(
        self,
        returncode: int,
        cmd: Sequence[str],
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
    ) -> None:
        super().__init__(returncode, cmd, stdout, stderr)


class InvalidVersion(Error, ValueError):
    """Invalid PostgreSQL version."""
