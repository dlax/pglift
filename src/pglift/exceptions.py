import abc


class NotFound(LookupError, metaclass=abc.ABCMeta):
    """Base class for object 'not found' errors."""

    @abc.abstractproperty
    def object_type(self) -> str:
        """Type of object that's not found."""
        raise NotImplementedError

    def show(self) -> str:
        """Return a human-readable error message."""
        return f"{self.object_type} '{self}' not found"


class InstanceNotFound(NotFound):
    """PostgreSQL instance not found or mis-configured."""

    object_type = "instance"


class RoleNotFound(NotFound):
    """PostgreSQL role not found."""

    object_type = "role"
