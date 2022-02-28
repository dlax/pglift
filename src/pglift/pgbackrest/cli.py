from typing import TYPE_CHECKING, Tuple

import click

from ..cli.instance import pass_pgbackrest_settings
from ..cli.util import Command, instance_identifier_option, pass_ctx
from .impl import make_cmd

if TYPE_CHECKING:
    from ..ctx import Context
    from ..models import system
    from ..settings import PgBackRestSettings


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
    cmd = make_cmd(instance, settings, *command)
    ctx.run(cmd, redirect_output=True, check=True)
