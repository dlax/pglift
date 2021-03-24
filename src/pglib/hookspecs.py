import pluggy

from .ctx import BaseContext
from .model import Instance

hookspec = pluggy.HookspecMarker("pglib")


@hookspec  # type: ignore[misc]
def instance_configure(ctx: BaseContext, instance: Instance) -> None:
    """Called when the PostgreSQL instance got (re-)configured."""


@hookspec  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Called when the PostgreSQL instance got dropped."""


@hookspec  # type: ignore[misc]
def instance_start(ctx: BaseContext, instance: Instance) -> None:
    """Called when the PostgreSQL instance got started."""
