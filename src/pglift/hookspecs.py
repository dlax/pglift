import pluggy
from pgtoolkit.conf import Configuration

from . import __name__ as pkgname
from .ctx import BaseContext
from .models import interface
from .models.system import Instance
from .types import ConfigChanges

hookspec = pluggy.HookspecMarker(pkgname)


@hookspec  # type: ignore[misc]
def instance_configure(
    ctx: BaseContext,
    manifest: interface.Instance,
    config: Configuration,
    changes: ConfigChanges,
) -> None:
    """Called when the PostgreSQL instance got (re-)configured."""


@hookspec  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Called when the PostgreSQL instance got dropped."""


@hookspec  # type: ignore[misc]
def instance_start(ctx: BaseContext, instance: Instance) -> None:
    """Called when the PostgreSQL instance got started."""


@hookspec  # type: ignore[misc]
def instance_stop(ctx: BaseContext, instance: Instance) -> None:
    """Called when the PostgreSQL instance got stopped."""
