import logging
import pathlib
import sys
from collections import OrderedDict
from functools import lru_cache, partial
from typing import List, Optional, Union

import click
import click.exceptions
import pydantic
import rich.logging
import rich.prompt
import rich.text
import rich.tree
from rich.console import Console
from rich.highlighter import NullHighlighter

from .. import __name__ as pkgname
from .. import _install, version
from .._compat import Literal
from ..ctx import Context, SiteMixin
from ..models import system
from ..settings import Settings, SiteSettings
from ..task import Displayer
from . import database, instance, pgconf, role
from .util import Group, pass_console, pass_ctx

logger = logging.getLogger(__name__)
CONSOLE = Console()


class InvalidSettingsError(click.ClickException):
    """Failure to load site settings."""

    def __init__(self, error: pydantic.ValidationError) -> None:
        message = f"invalid site settings\n{error}"
        super().__init__(message)


class LogDisplayer:
    def handle(self, msg: str) -> None:
        logger.info(msg)


class CLIContext(Context):
    """Default CLI context, non-interactive."""

    def confirm(self, message: str, default: bool) -> bool:
        return default


class CLISiteContext(SiteMixin, CLIContext):
    pass


class InteractiveCLIContext(CLISiteContext):
    """An interactive CLI context that prompts for confirmation."""

    def confirm(self, message: str, default: bool) -> bool:
        return rich.prompt.Confirm(console=CONSOLE).ask(
            f"[yellow]>[/yellow] {message}", default=default
        )

    @lru_cache(maxsize=None)
    def prompt(self, message: str, hide_input: bool = False) -> str:
        return rich.prompt.Prompt(console=CONSOLE).ask(
            f"[yellow]>[/yellow] {message}", password=hide_input
        )


class Obj:
    """Object bound to click.Context"""

    def __init__(
        self,
        *,
        context: Optional[CLIContext] = None,
        displayer: Optional[Displayer] = None,
        interactive: bool = True,
    ) -> None:
        if context is None:
            cls = InteractiveCLIContext if interactive else CLISiteContext
            try:
                settings = SiteSettings()
            except pydantic.ValidationError as e:
                raise InvalidSettingsError(e)
            context = cls(settings=settings)
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
            try:
                obj = context.ensure_object(Obj)
            except InvalidSettingsError:
                return []
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
    "--interactive/--non-interactive",
    default=True,
    help=(
        "Interactively prompt for confirmation when needed (the default), "
        "or automatically pick the default option for all choices."
    ),
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
    interactive: bool,
) -> None:
    """Deploy production-ready instances of PostgreSQL"""
    logger = logging.getLogger(pkgname)
    logger.setLevel(logging.DEBUG)
    handler: Union[logging.Handler, rich.logging.RichHandler]
    if log_file or not sys.stderr.isatty():
        if log_file:
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
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
        context.obj = Obj(
            displayer=None if log_file else LogDisplayer(),
            interactive=interactive,
        )
    else:
        assert isinstance(context.obj, Obj), context.obj


@cli.command("site-settings", hidden=True)
@click.option(
    "--schema", is_flag=True, help="Print the JSON Schema of site settings model."
)
@pass_console
@pass_ctx
def site_settings(ctx: Context, console: Console, schema: bool) -> None:
    """Show site settings."""
    if schema:
        console.print_json(Settings.schema_json())
    else:
        console.print_json(ctx.settings.json())


@cli.command(
    "site-configure",
    hidden=True,
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
    """Manage installation of extra data files for pglift.

    This is an INTERNAL command.
    """
    if action == "install":
        env = f"SETTINGS=@{settings}" if settings else None
        _install.do(ctx, env=env)
    elif action == "uninstall":
        _install.undo(ctx)


@cli.command("completion", hidden=True)
@click.argument("shell", type=click.Choice(["bash", "fish", "zsh"]), required=True)
def completion(shell: Literal["bash", "fish", "zsh"]) -> None:
    """Output completion for the given shell (bash, zsh or fish).

    To load completions:

    Bash:

    $ source <(pglift completion bash)

    To load completions for each session, execute once:

    $ pglift completion bash > /etc/bash_completion.d/pglift

    Fish:

    $ pglift completion fish | source

    To load completions for each session, execute once:

    $ pglift completion fish > ~/.config/fish/completions/pglift.fish

    Zsh:

    $ pglift completion zsh > "${fpath[1]}/pglift"
    """

    shell_complete_class_map = {
        "bash": click.shell_completion.BashComplete,
        "fish": click.shell_completion.FishComplete,
        "zsh": click.shell_completion.ZshComplete,
    }
    click.echo(
        shell_complete_class_map[shell](cli, {}, "pglift", "_PGLIFT_COMPLETE").source(),
        nl=False,
    )
