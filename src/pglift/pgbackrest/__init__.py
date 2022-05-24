import logging
from typing import TYPE_CHECKING, Dict

from .. import exceptions, hookimpl
from ..models import system
from . import impl
from .impl import available as available
from .impl import backup as backup
from .impl import expire as expire
from .impl import iter_backups as iter_backups
from .impl import restore as restore

if TYPE_CHECKING:
    import click
    from pgtoolkit.conf import Configuration

    from ..ctx import BaseContext
    from ..models import interface

__all__ = ["available", "backup", "expire", "iter_backups", "restore"]

logger = logging.getLogger(__name__)


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: "BaseContext",
    manifest: "interface.Instance",
    config: "Configuration",
    creating: bool,
) -> None:
    """Install pgBackRest for an instance when it gets configured."""
    settings = available(ctx)
    if not settings:
        logger.warning("pgbackrest not available, skipping backup configuration")
        return
    instance = system.Instance.system_lookup(ctx, (manifest.name, manifest.version))
    if instance.standby:
        return

    if creating and impl.enabled(instance, settings):
        if not ctx.confirm(
            f"Old pgbackrest repository exists for instance {instance}, continue by overwriting it?",
            False,
        ):
            raise exceptions.Cancelled("Pgbackrest repository already exists")
        impl.revert_setup(ctx, instance, settings, config)

    impl.setup(ctx, instance, settings, config)

    info = impl.backup_info(ctx, instance, settings)
    # Only initialize if the stanza does not already exist.
    if not info or info[0]["status"]["code"] == 1:
        impl.init(ctx, instance, settings)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: "BaseContext", instance: system.Instance) -> None:
    """Uninstall pgBackRest from an instance being dropped."""
    settings = available(ctx)
    if not settings:
        return
    if not impl.enabled(instance, settings):
        return

    nb_backups = len(impl.backup_info(ctx, instance, settings)[0]["backup"])

    if not nb_backups or ctx.confirm(
        f"Confirm deletion of {nb_backups} backup(s) for instance {instance} ?",
        True,
    ):
        impl.revert_setup(ctx, instance, settings, instance.config())


@hookimpl  # type: ignore[misc]
def instance_env(ctx: "BaseContext", instance: "system.Instance") -> Dict[str, str]:
    pgbackrest_settings = impl.available(ctx)
    if not pgbackrest_settings:
        return {}
    return impl.env_for(instance, pgbackrest_settings)


@hookimpl  # type: ignore[misc]
def cli() -> "click.Command":
    from .cli import pgbackrest

    return pgbackrest


@hookimpl  # type: ignore[misc]
def instance_cli(group: "click.Group") -> None:
    from .cli import instance_backup, instance_backups, instance_restore

    group.add_command(instance_backup)
    group.add_command(instance_backups)
    group.add_command(instance_restore)
