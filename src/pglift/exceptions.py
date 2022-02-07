import abc
import builtins
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:
    from .models.system import PostgreSQLInstance


class Error(Exception, metaclass=abc.ABCMeta):
    """Base class for operational error."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class Cancelled(Error):
    """Action cancelled."""


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


class SystemError(Error, OSError):
    """Error (unexpected state) on target system."""


class FileExistsError(SystemError, builtins.FileExistsError):
    pass


class FileNotFoundError(SystemError, builtins.FileNotFoundError):
    pass


class InvalidVersion(Error, ValueError):
    """Invalid PostgreSQL version."""


class InstanceAlreadyExists(Error, ValueError):
    """Instance with Name and version already exists"""


class InstanceStateError(Error, RuntimeError):
    """Unexpected instance state."""


class InstanceReadOnlyError(Error, RuntimeError):
    """Instance is a read-only standby."""

    def __init__(self, instance: "PostgreSQLInstance"):
        super().__init__(f"{instance} is a read-only standby instance")


class ConfigurationError(Error, LookupError):
    """A configuration entry is missing or invalid."""

    def __init__(self, path: Path, message: str) -> None:
        self.path = path  #: configuration file path
        super().__init__(message)

    def __str__(self) -> str:
        return f"{super().__str__()} (path: {self.path})"
