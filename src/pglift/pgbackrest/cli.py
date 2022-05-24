from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Optional, Tuple

import click

from .. import instances, types
from ..cli.instance import instance_identifier
from ..cli.util import (
    Command,
    as_json_option,
    instance_identifier_option,
    pass_component_settings,
    pass_console,
    pass_ctx,
    print_json_for,
    print_table_for,
)
from . import impl

if TYPE_CHECKING:
    from rich.console import Console

    from ..ctx import Context
    from ..models import system
    from ..settings import PgBackRestSettings

pass_pgbackrest_settings = partial(pass_component_settings, impl, "pgbackrest")


@click.command(
    "pgbackrest",
    hidden=True,
    cls=Command,
    context_settings={"ignore_unknown_options": True},
)
@instance_identifier_option
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
@pass_pgbackrest_settings
@pass_ctx
def pgbackrest(
    ctx: "Context",
    settings: "PgBackRestSettings",
    instance: "system.Instance",
    command: Tuple[str, ...],
) -> None:
    """Proxy to pgbackrest operations on an instance"""
    cmd = impl.make_cmd(instance, settings, *command)
    ctx.run(cmd, redirect_output=True, check=True)


@click.command("backup", cls=Command)
@instance_identifier(nargs=1)
@click.option(
    "--type",
    "backup_type",
    type=click.Choice([t.name for t in types.BackupType]),
    default=types.BackupType.default().name,
    help="Backup type",
    callback=lambda ctx, param, value: types.BackupType(value),
)
@pass_pgbackrest_settings
@pass_ctx
def instance_backup(
    ctx: "Context",
    settings: "PgBackRestSettings",
    instance: "system.Instance",
    backup_type: types.BackupType,
) -> None:
    """Back up PostgreSQL INSTANCE"""
    impl.backup(ctx, instance, settings, type=backup_type)


@click.command("restore", cls=Command)
@instance_identifier(nargs=1)
@click.option("--label", help="Label of backup to restore")
@click.option("--date", type=click.DateTime(), help="Date of backup to restore")
@pass_pgbackrest_settings
@pass_console
@pass_ctx
def instance_restore(
    ctx: "Context",
    console: "Console",
    settings: "PgBackRestSettings",
    instance: "system.Instance",
    label: Optional[str],
    date: Optional[datetime],
) -> None:
    """Restore PostgreSQL INSTANCE"""
    instances.check_status(ctx, instance, instances.Status.not_running)
    if label is not None and date is not None:
        raise click.BadArgumentUsage(
            "--label and --date arguments are mutually exclusive"
        )
    impl.restore(ctx, instance, settings, label=label, date=date)


@click.command("backups", cls=Command)
@as_json_option
@instance_identifier(nargs=1)
@pass_pgbackrest_settings
@pass_console
@pass_ctx
def instance_backups(
    ctx: "Context",
    console: "Console",
    settings: "PgBackRestSettings",
    instance: "system.Instance",
    as_json: bool,
) -> None:
    """List available backups for INSTANCE"""
    backups = impl.iter_backups(ctx, instance, settings)
    if as_json:
        print_json_for(
            (i.dict(by_alias=True) for i in backups), display=console.print_json
        )
    else:
        print_table_for(
            (i.dict(by_alias=True) for i in backups),
            title=f"Available backups for instance {instance}",
            display=console.print,
        )
