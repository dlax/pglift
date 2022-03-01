import logging
import pathlib
from collections import OrderedDict
from functools import partial
from typing import List, Optional, Union

import click
import click.exceptions
import rich.logging
import rich.prompt
import rich.text
import rich.tree
from rich.console import Console
from rich.highlighter import NullHighlighter
from typing_extensions import Literal

from .. import __name__ as pkgname
from .. import _install, version
from ..ctx import Context
from ..models import system
from ..settings import Settings
from ..task import Displayer
from . import database, instance, pgconf, role
from .util import Group, pass_console, pass_ctx

logger = logging.getLogger(__name__)
CONSOLE = Console()


class LogDisplayer:
    def handle(self, msg: str) -> None:
        logger.info(msg)


class CLIContext(Context):
    def confirm(self, message: str, default: bool) -> bool:
        return rich.prompt.Confirm(console=CONSOLE).ask(f"[yellow]>[/yellow] {message}")


class Obj:
    """Object bound to click.Context"""

    def __init__(
        self,
        *,
        context: Optional[CLIContext] = None,
        displayer: Optional[Displayer] = None,
    ) -> None:
        if context is None:
            context = CLIContext(settings=Settings())
        self.ctx = context
        self.displayer = displayer
        self.console = CONSOLE
        # Set in commands taking a -i/--instance option through
        # instance_identifier_option decorator's callback.
        self.instance: Optional[system.Instance] = None


class CLIGroup(Group):
    """Group gathering main commands (defined here), commands from submodules
    and commands from plugins.
    """

    submodules = OrderedDict(
        [
            ("instance", instance.cli),
            ("pgconf", pgconf.cli),
            ("role", role.cli),
            ("database", database.cli),
        ]
    )

    def list_commands(self, context: click.Context) -> List[str]:
        main_commands = super().list_commands(context)
        obj = context.obj
        if obj is None:
            obj = context.ensure_object(Obj)
        plugins_commands = sorted(g.name for g in obj.ctx.hook.cli())
        return main_commands + list(self.submodules) + plugins_commands

    def get_command(
        self, context: click.Context, cmd_name: str
    ) -> Optional[click.Command]:
        main_command = super().get_command(context, cmd_name)
        if main_command is not None:
            return main_command
        try:
            cmd = self.submodules[cmd_name]
        except KeyError:
            pass
        else:
            assert isinstance(cmd, click.Command), cmd
            return cmd
        obj = context.obj
        if obj is None:
            obj = context.ensure_object(Obj)
        for group in obj.ctx.hook.cli():
            assert isinstance(group, click.Command)
            if group.name == cmd_name:
                return group
        return None


def print_version(context: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or context.resilient_parsing:
        return
    click.echo(f"pglift version {version()}")
    context.exit()


@click.group(cls=CLIGroup)
@click.option(
    "-L",
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default=None,
    help="Set log threshold (default to INFO when logging to stderr or WARNING when logging to a file).",
)
@click.option(
    "-l",
    "--log-file",
    type=click.Path(dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    metavar="LOGFILE",
    help="Write logs to LOGFILE, instead of stderr.",
)
@click.option(
    "--version",
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Show program version.",
)
@click.pass_context
def cli(
    context: click.Context,
    log_level: Optional[str],
    log_file: Optional[pathlib.Path],
) -> None:
    """Deploy production-ready instances of PostgreSQL"""
    logger = logging.getLogger(pkgname)
    logger.setLevel(logging.DEBUG)
    handler: Union[logging.Handler, rich.logging.RichHandler]
    if log_file:
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%X"
        )
        handler.setFormatter(formatter)
        handler.setLevel(log_level or logging.WARNING)
    else:
        handler = rich.logging.RichHandler(
            level=log_level or logging.INFO,
            console=Console(stderr=True),
            show_time=False,
            show_path=False,
            highlighter=NullHighlighter(),
        )
    logger.addHandler(handler)
    # Remove rich handler on close since this would pollute all tests stderr
    # otherwise.
    context.call_on_close(partial(logger.removeHandler, handler))

    if not context.obj:
        context.obj = Obj(displayer=None if log_file else LogDisplayer())
    else:
        assert isinstance(context.obj, Obj), context.obj


@cli.command("site-settings", hidden=True)
@pass_console
@pass_ctx
def site_settings(ctx: Context, console: Console) -> None:
    """Show site settings."""
    console.print_json(ctx.settings.json())


@cli.command(
    "site-configure",
    hidden=True,
    help="Manage installation of extra data files for pglift.\n\nThis is an INTERNAL command.",
)
@click.argument(
    "action", type=click.Choice(["install", "uninstall"]), default="install"
)
@click.option(
    "--settings",
    type=click.Path(exists=True, path_type=pathlib.Path),
    help="Custom settings file.",
)
@pass_ctx
def site_configure(
    ctx: Context,
    action: Literal["install", "uninstall"],
    settings: Optional[pathlib.Path],
) -> None:
    if action == "install":
        env = f"SETTINGS=@{settings}" if settings else None
        _install.do(ctx, env=env)
    elif action == "uninstall":
        _install.undo(ctx)