from typing import TYPE_CHECKING, Any, Dict, Optional, Type

import pluggy
from pgtoolkit.conf import Configuration

from . import __name__ as pkgname

if TYPE_CHECKING:
    import click

    from .ctx import BaseContext
    from .models import interface
    from .models.system import BaseInstance, Instance
    from .types import ConfigChanges, ServiceManifest

hookspec = pluggy.HookspecMarker(pkgname)


@hookspec  # type: ignore[misc]
def install_systemd_unit_template(ctx: "BaseContext", header: str = "") -> None:
    """Install systemd unit templates."""


@hookspec  # type: ignore[misc]
def uninstall_systemd_unit_template(ctx: "BaseContext") -> None:
    """Uninstall systemd unit templates."""


@hookspec  # type: ignore[misc]
def cli() -> "click.Command":
    """Return command-line entry point as click Command (or Group) for the plugin."""


@hookspec  # type: ignore[misc]
def instance_cli(group: "click.Group") -> None:
    """Extend 'group' with extra commands from the plugin."""


@hookspec  # type: ignore[misc]
def system_lookup(ctx: "BaseContext", instance: "BaseInstance") -> Optional[Any]:
    """Look up for the satellite service object on system that matches specified instance."""


@hookspec  # type: ignore[misc]
def get(ctx: "BaseContext", instance: "Instance") -> Optional["ServiceManifest"]:
    """Return the description the satellite service bound to specified instance."""


@hookspec  # type: ignore[misc]
def interface_model() -> Type["ServiceManifest"]:
    """The interface model for satellite component provided plugin."""


@hookspec  # type: ignore[misc]
def instance_configuration(
    ctx: "BaseContext", manifest: "interface.Instance"
) -> Configuration:
    """Called before the PostgreSQL instance configuration is written."""


@hookspec  # type: ignore[misc]
def instance_configure(
    ctx: "BaseContext",
    manifest: "interface.Instance",
    config: Configuration,
    changes: "ConfigChanges",
    creating: bool,
) -> None:
    """Called when the PostgreSQL instance got (re-)configured."""


@hookspec  # type: ignore[misc]
def instance_drop(ctx: "BaseContext", instance: "Instance") -> None:
    """Called when the PostgreSQL instance got dropped."""


@hookspec  # type: ignore[misc]
def instance_start(ctx: "BaseContext", instance: "Instance") -> None:
    """Called when the PostgreSQL instance got started."""


@hookspec  # type: ignore[misc]
def instance_stop(ctx: "BaseContext", instance: "Instance") -> None:
    """Called when the PostgreSQL instance got stopped."""


@hookspec  # type: ignore[misc]
def instance_env(ctx: "BaseContext", instance: "Instance") -> Dict[str, str]:
    """Return environment variables for instance defined by the plugin."""
