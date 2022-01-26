import json
import logging
import os
import pathlib
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime
from functools import partial, wraps
from types import ModuleType
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import click
import psycopg
import pydantic.json
import rich.logging
import rich.prompt
import rich.text
import rich.tree
from click.exceptions import Exit
from pydantic.utils import deep_update
from rich.console import Console, RenderableType
from rich.highlighter import NullHighlighter
from rich.live import Live
from rich.table import Table
from typing_extensions import Literal

from . import __name__ as pkgname
from . import _install, conf, databases, exceptions
from . import instance as instance_mod
from . import logger, pgbackrest, pm, privileges, prometheus, roles, task, version
from ._compat import nullcontext
from .ctx import Context
from .instance import Status
from .models import helpers, interface
from .models.system import Instance
from .settings import POSTGRESQL_SUPPORTED_VERSIONS, Settings
from .task import Displayer
from .types import ConfigChanges

console = Console()


class Obj:
    """Object bound to click.Context"""

    def __init__(self, context: Context, displayer: Optional[Displayer]) -> None:
        self.ctx = context
        self.displayer = displayer


def pass_ctx(f: Callable[..., Any]) -> Callable[..., Any]:
    """Command decorator passing 'Context' bound to click.Context's object."""

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        context = click.get_current_context()
        ctx = context.obj.ctx
        assert isinstance(ctx, Context), ctx
        return context.invoke(f, ctx, *args, **kwargs)

    return wrapper


def pass_displayer(f: Callable[..., Any]) -> Callable[..., Any]:
    """Command decorator passing 'displayer' bound to click.Context's object.

    If we have the "foreground" option set, we cannot really use a displayer
    because its __exit__() code would never be called as we use os.execv().
    Thus we disable the displayer in those cases.
    """

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        context = click.get_current_context()
        displayer = context.obj.displayer
        if displayer is None or kwargs.get("foreground", False):
            displayer = nullcontext()
        return context.invoke(f, displayer, *args, **kwargs)

    return wrapper


class Command(click.Command):
    def invoke(self, context: click.Context) -> Any:
        ctx = context.obj.ctx
        displayer = context.obj.displayer
        logger = logging.getLogger(pkgname)
        logdir = ctx.settings.logpath
        logdir.mkdir(parents=True, exist_ok=True)
        logfilename = f"{time.time()}.log"
        logfile = logdir / logfilename
        try:
            handler = logging.FileHandler(logfile)
        except OSError:
            # Might be, e.g. PermissionError, if log file path is not writable.
            logfile = pathlib.Path(
                tempfile.NamedTemporaryFile(prefix="pglift", suffix=logfilename).name
            )
            handler = logging.FileHandler(logfile)
        formatter = logging.Formatter(
            fmt="%(levelname)-8s - %(asctime)s - %(name)s:%(filename)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        keep_logfile = False
        try:
            try:
                with task.displayer_installed(displayer):
                    return super().invoke(context)
            except exceptions.Error as e:
                logger.debug("an internal error occurred", exc_info=True)
                msg = str(e)
                if isinstance(e, exceptions.CommandError):
                    if e.stderr:
                        msg += f"\n{e.stderr}"
                    if e.stdout:
                        msg += f"\n{e.stdout}"
                raise click.ClickException(msg)
            except (click.ClickException, click.Abort, click.exceptions.Exit):
                raise
            except pydantic.ValidationError as e:
                logger.debug("a validation error occurred", exc_info=True)
                raise click.ClickException(str(e))
            except psycopg.OperationalError as e:
                logger.debug("an operational error occurred", exc_info=True)
                raise click.ClickException(str(e).strip())
            except Exception:
                keep_logfile = True
                logger.exception("an unexpected error occurred")
                raise click.ClickException(
                    "an unexpected error occurred, this is probably a bug; "
                    f"details can be found at {logfile}"
                )
        finally:
            if not keep_logfile:
                os.unlink(logfile)
                if next(logfile.parent.iterdir(), None) is None:
                    logfile.parent.rmdir()


class Group(click.Group):
    command_class = Command
    group_class = type


C = TypeVar("C", bound=Callable[..., Any])


def require_component(mod: ModuleType, name: str, fn: C) -> C:
    @wraps(fn)
    def wrapper(ctx: Context, *args: Any, **kwargs: Any) -> None:
        if not getattr(mod, "available")(ctx):
            click.echo(f"{name} not available", err=True)
            raise Exit(1)
        fn(ctx, *args, **kwargs)

    return cast(C, wrapper)


require_pgbackrest = partial(require_component, pgbackrest, "pgbackrest")
require_prometheus = partial(
    require_component, prometheus, "Prometheus postgres_exporter"
)


def get_instance(ctx: Context, name: str, version: Optional[str]) -> Instance:
    """Return an Instance from name/version, possibly guessing version if unspecified."""
    if version is None:
        found = None
        for version in POSTGRESQL_SUPPORTED_VERSIONS:
            try:
                instance = Instance.system_lookup(ctx, (name, version))
            except exceptions.InstanceNotFound:
                logger.debug("instance '%s' not found in version %s", name, version)
            else:
                logger.info("instance '%s' found in version %s", name, version)
                if found:
                    raise click.BadParameter(
                        f"instance '{name}' exists in several PostgreSQL versions;"
                        " please select version explicitly"
                    )
                found = instance

        if found:
            return found

        raise click.BadParameter(f"instance '{name}' not found")

    try:
        return Instance.system_lookup(ctx, (name, version))
    except Exception as e:
        raise click.BadParameter(str(e))


def nameversion_from_id(instance_id: str) -> Tuple[str, Optional[str]]:
    version = None
    try:
        version, name = instance_id.split("/", 1)
    except ValueError:
        name = instance_id
    return name, version


def instance_lookup(
    context: click.Context, param: click.Parameter, value: str
) -> Instance:
    name, version = nameversion_from_id(value)
    ctx = context.obj.ctx
    return get_instance(ctx, name, version)


instance_identifier = click.argument(
    "instance", metavar="<version>/<name>", callback=instance_lookup
)


class LiveDisplayer(Live):
    """Render nested operations as a grid and live update their status.

    >>> from contextlib import suppress
    >>> with LiveDisplayer(console=Console(), width=50) as d, \
                suppress(ZeroDivisionError):
    ...     with d.handle("compute something"):
    ...         x = 1 + 1
    ...         with d.handle("use the result in another computation"):
    ...             y = x + 1
    ...         with d.handle("now, something harder"):
    ...             z = y / 0
    ...     with d.handle("should not run"):
    ...         assert False
    compute something...........................[FAIL]
     use the result in another computation......[ OK ]
     now, something harder......................[FAIL]
    """

    ok = rich.text.Text("[ OK ]")
    ok.stylize("green", 1, 5)
    fail = rich.text.Text("[FAIL]")
    fail.stylize("red", 1, 5)
    intr = rich.text.Text("[INTR]")
    intr.stylize("yellow", 1, 5)

    def __init__(self, *, console: Console, width: Optional[int] = None) -> None:
        self.grid = Table.grid()
        super().__init__(self.grid, console=console)
        self._level = 0
        self._width = width or self.console.size.width

    @contextmanager
    def handle(self, msg: str) -> Iterator[None]:
        """Register 'msg' as the current (running) operation."""
        text = rich.text.Text(" " * self._level + msg)
        text.align("left", self._width - 6, ".")
        self._level += 1
        self.grid.add_row(text)
        try:
            yield None
        except KeyboardInterrupt:
            tail = self.intr
            raise
        except Exception:
            tail = self.fail
            raise
        else:
            tail = self.ok
        finally:
            text.append_text(tail)
            self._level -= 1


_M = TypeVar("_M", bound=pydantic.BaseModel)


def print_table_for(
    items: Iterable[_M],
    title: Optional[str] = None,
    *,
    display: Callable[[RenderableType], None] = console.print,
) -> None:
    """Render a list of items as a table.

    >>> class Address(pydantic.BaseModel):
    ...     street: str
    ...     zipcode: int = pydantic.Field(alias="zip")
    ...     city: str
    >>> class Person(pydantic.BaseModel):
    ...     name: str
    ...     address: Address
    >>> items = [Person(name="bob",
    ...                 address=Address(street="main street", zip=31234, city="luz"))]
    >>> print_table_for(items, title="address book", display=rich.print)  # doctest: +NORMALIZE_WHITESPACE
                   address book
    ┏━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┓
    ┃      ┃ address     ┃ address ┃ address ┃
    ┃ name ┃ street      ┃ zip     ┃ city    ┃
    ┡━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━┩
    │ bob  │ main street │ 31234   │ luz     │
    └──────┴─────────────┴─────────┴─────────┘
    """
    table = None
    headers: List[str] = []
    rows = []
    for item in items:
        d = item.dict(by_alias=True)
        row = []
        hdr = []
        for k, v in list(d.items()):
            if isinstance(v, dict):
                for sk, sv in v.items():
                    mk = f"{k}\n{sk}"
                    hdr.append(mk)
                    row.append(sv)
            else:
                hdr.append(k)
                row.append(v)
        if not headers:
            headers = hdr[:]
        rows.append([str(v) for v in row])
    if not rows:
        return
    table = Table(*headers, title=title)
    for row in rows:
        table.add_row(*row)
    display(table)


def print_json_for(
    items: Iterable[_M], *, display: Callable[[str], None] = console.print_json
) -> None:
    """Render a list of items as JSON.

    >>> class Foo(pydantic.BaseModel):
    ...     bar_: str = pydantic.Field(alias="bar")
    ...     baz: int
    >>> items = [Foo(bar="x", baz=1), Foo(bar="y", baz=3)]
    >>> print_json_for(items, display=rich.print)
    [{"bar": "x", "baz": 1}, {"bar": "y", "baz": 3}]
    """
    display(
        json.dumps(
            [i.dict(by_alias=True) for i in items],
            default=pydantic.json.pydantic_encoder,
        ),
    )


as_json_option = click.option("--json", "as_json", is_flag=True, help="Print as JSON")


def validate_foreground(
    context: click.Context, param: click.Parameter, value: bool
) -> bool:
    ctx = context.obj.ctx
    if ctx.settings.service_manager == "systemd" and value:
        raise click.BadParameter("cannot be used with systemd")
    return value


foreground_option = click.option(
    "--foreground",
    is_flag=True,
    help="Start the program in foreground.",
    callback=validate_foreground,
)


def print_version(context: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or context.resilient_parsing:
        return
    click.echo(f"pglift version {version()}")
    context.exit()


@click.group(cls=Group)
@click.option(
    "-L",
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="warning",
    help="Set log threshold",
)
@click.option(
    "-l",
    "--log-file",
    type=click.Path(dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    metavar="LOGFILE",
    help="Write log to LOGFILE",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=False,
    help="Disable status output.",
)
@click.option(
    "--settings",
    "settings_file",
    type=click.Path(resolve_path=True, path_type=pathlib.Path),
    hidden=True,
    help="Settings file, overriding local site configuration.",
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
    log_level: str,
    log_file: Optional[pathlib.Path],
    quiet: bool,
    settings_file: Optional[pathlib.Path],
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
        handler.setLevel(log_level)
    else:
        handler = rich.logging.RichHandler(
            level=log_level,
            console=Console(stderr=True),
            show_time=True,
            log_time_format="%X",
            omit_repeated_times=False,
            show_path=False,
            highlighter=NullHighlighter(),
        )
    logger.addHandler(handler)
    # Remove rich handler on close since this would pollute all tests stderr
    # otherwise.
    context.call_on_close(partial(logger.removeHandler, handler))

    if not context.obj:
        settings = None
        if settings_file is not None:
            settings = Settings.parse_file(settings_file)
        context.obj = Obj(
            Context(plugin_manager=pm.PluginManager.get(), settings=settings),
            LiveDisplayer(console=console) if not quiet else None,
        )
    else:
        assert isinstance(context.obj, Obj), context.obj


@cli.command("site-settings", hidden=True)
@pass_ctx
def site_settings(ctx: Context) -> None:
    """Show site settings."""
    rich.print_json(ctx.settings.json())


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
@pass_displayer
@pass_ctx
def site_configure(
    ctx: Context,
    displayer: Displayer,
    action: Literal["install", "uninstall"],
    settings: Optional[pathlib.Path],
) -> None:
    with displayer:
        if action == "install":
            env = f"SETTINGS=@{settings}" if settings else None
            _install.do(ctx, env=env)
        elif action == "uninstall":
            _install.undo(ctx)


@cli.group("instance")
def instance() -> None:
    """Manipulate instances"""


@instance.command("init")
@helpers.parameters_from_model(interface.Instance)
@pass_displayer
@pass_ctx
def instance_init(
    ctx: Context, displayer: Displayer, instance: interface.Instance
) -> None:
    """Initialize a PostgreSQL instance"""
    if instance_mod.exists(ctx, instance.name, instance.version):
        raise click.ClickException("instance already exists")
    with displayer, task.transaction():
        instance_mod.apply(ctx, instance)


def maybe_restart(
    ctx: Context, displayer: Displayer, r: instance_mod.ApplyResult
) -> None:
    if r is None:
        return
    instance, changes, needs_restart = r
    if not needs_restart:
        return
    if rich.prompt.Confirm.ask(
        "[cyan]Instance needs to be restarted.[/cyan]\n  Restart now?",
    ):
        with displayer:
            instance_mod.restart(ctx, instance)


@instance.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_displayer
@pass_ctx
def instance_apply(ctx: Context, displayer: Displayer, file: IO[str]) -> None:
    """Apply manifest as a PostgreSQL instance"""
    instance = interface.Instance.parse_yaml(file)
    with displayer:
        r = instance_mod.apply(ctx, instance)
    maybe_restart(ctx, displayer, r)


@instance.command("alter")
@instance_identifier
@helpers.parameters_from_model(
    interface.Instance, exclude=["name", "version"], parse_model=False
)
@pass_displayer
@pass_ctx
def instance_alter(
    ctx: Context,
    displayer: Displayer,
    instance: Instance,
    **changes: Any,
) -> None:
    """Alter a PostgreSQL instance"""
    changes = helpers.unnest(interface.Instance, changes)
    values = instance_mod.describe(ctx, instance.name, instance.version).dict()
    values = deep_update(values, changes)
    altered = interface.Instance.parse_obj(values)
    with displayer:
        r = instance_mod.apply(ctx, altered)
    maybe_restart(ctx, displayer, r)


@instance.command("promote")
@instance_identifier
@pass_displayer
@pass_ctx
def instance_promote(ctx: Context, displayer: Displayer, instance: Instance) -> None:
    """Promote a standby PostgreSQL instance"""
    with displayer:
        instance_mod.promote(ctx, instance)


@instance.command("schema")
def instance_schema() -> None:
    """Print the JSON schema of PostgreSQL instance model"""
    console.print_json(interface.Instance.schema_json(indent=2))


@instance.command("describe")
@instance_identifier
@pass_ctx
def instance_describe(ctx: Context, instance: Instance) -> None:
    """Describe a PostgreSQL instance"""
    described = instance_mod.describe(ctx, instance.name, instance.version)
    click.echo(described.yaml(), nl=False)


@instance.command("list")
@click.option(
    "--version",
    type=click.Choice(POSTGRESQL_SUPPORTED_VERSIONS),
    help="Only list instances of specified version.",
)
@as_json_option
@pass_displayer
@pass_ctx
def instance_list(
    ctx: Context, displayer: Displayer, version: Optional[str], as_json: bool
) -> None:
    """List the available instances"""

    instances = instance_mod.list(ctx, version=version)
    if as_json:
        print_json_for(instances)
    else:
        print_table_for(instances)


@instance.group("config")
def instance_configure() -> None:
    """Manage configuration of a PostgreSQL instance."""


def show_configuration_changes(
    changes: ConfigChanges, parameters: Iterable[str]
) -> None:
    for param, (old, new) in changes.items():
        click.secho(f"{param}: {old} -> {new}", err=True, fg="green")
    unchanged = set(parameters) - set(changes)
    if unchanged:
        click.secho(
            f"changes in {', '.join(map(repr, sorted(unchanged)))} not applied",
            err=True,
            fg="red",
        )
        click.secho(
            " hint: either these changes have no effect (values already set) "
            "or specified parameters are already defined in an un-managed file "
            "(e.g. 'postgresql.conf')",
            err=True,
            fg="blue",
        )


@instance_configure.command("show")
@instance_identifier
@click.argument("parameter", nargs=-1)
@pass_ctx
def instance_configure_show(
    ctx: Context, instance: Instance, parameter: Tuple[str]
) -> None:
    """Show configuration (all parameters or specified ones).

    Only uncommented parameters are shown when no PARAMETER is specified. When
    specific PARAMETERs are queried, commented values are also shown.
    """
    config = instance.config()
    for entry in config.entries.values():
        if parameter:
            if entry.name in parameter:
                if entry.commented:
                    click.echo(f"# {entry.name} = {entry.serialize()}")
                else:
                    click.echo(f"{entry.name} = {entry.serialize()}")
        elif not entry.commented:
            click.echo(f"{entry.name} = {entry.serialize()}")


def validate_configuration_parameters(
    context: click.Context, param: click.Parameter, value: Tuple[str]
) -> Dict[str, str]:
    items = {}
    for v in value:
        try:
            key, val = v.split("=", 1)
        except ValueError:
            raise click.BadParameter(v)
        items[key] = val
    return items


@instance_configure.command("set")
@instance_identifier
@click.argument(
    "parameters",
    metavar="<PARAMETER>=<VALUE>",
    nargs=-1,
    callback=validate_configuration_parameters,
)
@pass_ctx
def instance_configure_set(
    ctx: Context, instance: Instance, parameters: Dict[str, Any]
) -> None:
    """Set configuration items."""
    values = instance.config(managed_only=True).as_dict()
    values.update(parameters)
    manifest = interface.Instance(name=instance.name, version=instance.version)
    changes = instance_mod.configure(ctx, manifest, values=values)
    show_configuration_changes(changes, parameters.keys())


@instance_configure.command("remove")
@instance_identifier
@click.argument("parameters", nargs=-1)
@pass_ctx
def instance_configure_remove(
    ctx: Context, instance: Instance, parameters: Tuple[str]
) -> None:
    """Remove configuration items."""
    values = instance.config(managed_only=True).as_dict()
    for p in parameters:
        try:
            del values[p]
        except KeyError:
            raise click.ClickException(f"'{p}' not found in managed configuration")
    manifest = interface.Instance(name=instance.name, version=instance.version)
    changes = instance_mod.configure(ctx, manifest, values=values)
    show_configuration_changes(changes, parameters)


@instance_configure.command("edit")
@instance_identifier
@pass_ctx
def instance_configure_edit(ctx: Context, instance: Instance) -> None:
    """Edit managed configuration."""
    confd = conf.info(instance.datadir)[0]
    click.edit(filename=str(confd / "user.conf"))


@instance.command("drop")
@instance_identifier
@pass_displayer
@pass_ctx
def instance_drop(ctx: Context, displayer: Displayer, instance: Instance) -> None:
    """Drop a PostgreSQL instance"""
    with displayer:
        instance_mod.drop(ctx, instance)


@instance.command("status")
@instance_identifier
@pass_displayer
@click.pass_context
def instance_status(
    context: click.Context, displayer: Displayer, instance: Instance
) -> None:
    """Check the status of a PostgreSQL instance.

    Output the status string value ('running', 'not running', 'unspecified
    datadir') and exit with respective status code (0, 3, 4).
    """
    ctx = context.obj.ctx
    with displayer:
        status = instance_mod.status(ctx, instance)
    click.echo(status.name.replace("_", " "))
    context.exit(status.value)


@instance.command("start")
@instance_identifier
@foreground_option
@pass_displayer
@pass_ctx
def instance_start(
    ctx: Context, displayer: Displayer, instance: Instance, foreground: bool
) -> None:
    """Start a PostgreSQL instance"""
    instance_mod.check_status(ctx, instance, Status.not_running)
    with displayer:
        instance_mod.start(ctx, instance, foreground=foreground)


@instance.command("stop")
@instance_identifier
@pass_displayer
@pass_ctx
def instance_stop(ctx: Context, displayer: Displayer, instance: Instance) -> None:
    """Stop a PostgreSQL instance"""
    with displayer:
        instance_mod.stop(ctx, instance)


@instance.command("reload")
@instance_identifier
@pass_displayer
@pass_ctx
def instance_reload(ctx: Context, displayer: Displayer, instance: Instance) -> None:
    """Reload a PostgreSQL instance"""
    with displayer:
        instance_mod.reload(ctx, instance)


@instance.command("restart")
@instance_identifier
@pass_displayer
@pass_ctx
def instance_restart(ctx: Context, displayer: Displayer, instance: Instance) -> None:
    """Restart a PostgreSQL instance"""
    with displayer:
        instance_mod.restart(ctx, instance)


@instance.command("exec")
@instance_identifier
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
@pass_ctx
def instance_exec(ctx: Context, instance: Instance, command: Tuple[str, ...]) -> None:
    """Execute command in the libpq environment for a PostgreSQL instance."""
    if not command:
        raise click.ClickException("no command given")
    instance_mod.exec(ctx, instance, command)


@instance.command("env")
@instance_identifier
@pass_ctx
def instance_env(ctx: Context, instance: Instance) -> None:
    """Output environment variables suitable to connect to a PostgreSQL instance.

    This can be injected in shell using:

    export $(pglift instance env myinstance)
    """
    for key, value in sorted(instance_mod.env_for(ctx, instance, path=True).items()):
        click.echo(f"{key}={value}")


@instance.command("logs")
@instance_identifier
@pass_ctx
def instance_logs(ctx: Context, instance: Instance) -> None:
    """Output instance logs

    This assumes that the PostgreSQL instance is configured to use file-based
    logging (i.e. log_destination amongst 'stderr' or 'csvlog').
    """
    for line in instance_mod.logs(ctx, instance):
        click.echo(line, nl=False)


@instance.command("backup")
@instance_identifier
@click.option(
    "--type",
    "backup_type",
    type=click.Choice([t.name for t in pgbackrest.BackupType]),
    default=pgbackrest.BackupType.default().name,
    help="Backup type",
    callback=lambda ctx, param, value: pgbackrest.BackupType(value),
)
@pass_displayer
@pass_ctx
@require_pgbackrest
def instance_backup(
    ctx: Context,
    displayer: Displayer,
    instance: Instance,
    backup_type: pgbackrest.BackupType,
) -> None:
    """Back up a PostgreSQL instance"""
    with displayer:
        pgbackrest.backup(ctx, instance, type=backup_type)


@instance.command("restore")
@instance_identifier
@click.option(
    "-l",
    "--list",
    "list_only",
    is_flag=True,
    default=False,
    help="Only list available backups",
)
@click.option("--label", help="Label of backup to restore")
@click.option("--date", type=click.DateTime(), help="Date of backup to restore")
@pass_displayer
@pass_ctx
@require_pgbackrest
def instance_restore(
    ctx: Context,
    displayer: Displayer,
    instance: Instance,
    list_only: bool,
    label: Optional[str],
    date: Optional[datetime],
) -> None:
    """Restore a PostgreSQL instance"""
    if list_only:
        backups = pgbackrest.iter_backups(ctx, instance)
        print_table_for(backups, title=f"Available backups for instance {instance}")
    else:
        with displayer:
            instance_mod.check_status(ctx, instance, Status.not_running)
            if label is not None and date is not None:
                raise click.BadArgumentUsage(
                    "--label and --date arguments are mutually exclusive"
                )
            pgbackrest.restore(ctx, instance, label=label, date=date)


@instance.command("privileges")
@instance_identifier
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@as_json_option
@pass_ctx
def instance_privileges(
    ctx: Context,
    instance: Instance,
    databases: Sequence[str],
    roles: Sequence[str],
    as_json: bool,
) -> None:
    """List default privileges on instance."""
    with instance_mod.running(ctx, instance):
        try:
            prvlgs = privileges.get(ctx, instance, databases=databases, roles=roles)
        except ValueError as e:
            raise click.ClickException(str(e))
    if as_json:
        print_json_for(prvlgs)
    else:
        print_table_for(prvlgs, title=f"Default privileges on instance {instance}")


@instance.command("upgrade")
@instance_identifier
@click.argument("newversion", required=False, type=click.STRING)
@click.argument("newname", required=False, type=click.STRING)
@click.option("--port", required=False, type=click.INT)
@click.option(
    "--jobs",
    required=False,
    type=click.INT,
    help="number of simultaneous processes or threads to use (from pg_upgrade)",
)
@pass_displayer
@pass_ctx
def instance_upgrade(
    ctx: Context,
    displayer: Displayer,
    instance: Instance,
    newversion: Optional[str],
    newname: Optional[str],
    port: Optional[int],
    jobs: Optional[int],
) -> None:
    """Upgrade an instance using pg_upgrade"""
    instance_mod.check_status(ctx, instance, Status.not_running)
    with displayer:
        new_instance = instance_mod.upgrade(
            ctx, instance, version=newversion, name=newname, port=port, jobs=jobs
        )
    instance_mod.start(ctx, new_instance)


@cli.group("role")
def role() -> None:
    """Manipulate roles"""


@role.command("create")
@instance_identifier
@helpers.parameters_from_model(interface.Role)
@pass_displayer
@pass_ctx
def role_create(
    ctx: Context, displayer: Displayer, instance: Instance, role: interface.Role
) -> None:
    """Create a role in a PostgreSQL instance"""
    with instance_mod.running(ctx, instance):
        if roles.exists(ctx, instance, role.name):
            raise click.ClickException("role already exists")
        with displayer, task.transaction():
            roles.apply(ctx, instance, role)


@role.command("alter")
@instance_identifier
@helpers.parameters_from_model(interface.Role, parse_model=False)
@pass_displayer
@pass_ctx
def role_alter(
    ctx: Context, displayer: Displayer, instance: Instance, name: str, **changes: Any
) -> None:
    """Alter a role in a PostgreSQL instance"""
    changes = helpers.unnest(interface.Role, changes)
    with instance_mod.running(ctx, instance):
        values = roles.describe(ctx, instance, name).dict()
        values = deep_update(values, changes)
        altered = interface.Role.parse_obj(values)
        with displayer:
            roles.apply(ctx, instance, altered)


@role.command("schema")
def role_schema() -> None:
    """Print the JSON schema of role model"""
    console.print_json(interface.Role.schema_json(indent=2))


@role.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_displayer
@pass_ctx
def role_apply(
    ctx: Context, displayer: Displayer, instance: Instance, file: IO[str]
) -> None:
    """Apply manifest as a role"""
    role = interface.Role.parse_yaml(file)
    with displayer, instance_mod.running(ctx, instance):
        roles.apply(ctx, instance, role)


@role.command("describe")
@instance_identifier
@click.argument("name")
@pass_ctx
def role_describe(ctx: Context, instance: Instance, name: str) -> None:
    """Describe a role"""
    with instance_mod.running(ctx, instance):
        described = roles.describe(ctx, instance, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@role.command("drop")
@instance_identifier
@click.argument("name")
@pass_displayer
@pass_ctx
def role_drop(
    ctx: Context, displayer: Displayer, instance: Instance, name: str
) -> None:
    """Drop a role"""
    with instance_mod.running(ctx, instance), displayer:
        roles.drop(ctx, instance, name)


@role.command("privileges")
@instance_identifier
@click.argument("name")
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@as_json_option
@pass_ctx
def role_privileges(
    ctx: Context, instance: Instance, name: str, databases: Sequence[str], as_json: bool
) -> None:
    """List default privileges of a role."""
    with instance_mod.running(ctx, instance):
        roles.describe(ctx, instance, name)  # check existence
        try:
            prvlgs = privileges.get(ctx, instance, databases=databases, roles=(name,))
        except ValueError as e:
            raise click.ClickException(str(e))
    if as_json:
        print_json_for(prvlgs)
    else:
        print_table_for(prvlgs)


@cli.group("database")
def database() -> None:
    """Manipulate databases"""


@database.command("create")
@instance_identifier
@helpers.parameters_from_model(interface.Database)
@pass_displayer
@pass_ctx
def database_create(
    ctx: Context, displayer: Displayer, instance: Instance, database: interface.Database
) -> None:
    """Create a database in a PostgreSQL instance"""
    with instance_mod.running(ctx, instance):
        if databases.exists(ctx, instance, database.name):
            raise click.ClickException("database already exists")
        with displayer, task.transaction():
            databases.apply(ctx, instance, database)


@database.command("alter")
@instance_identifier
@helpers.parameters_from_model(interface.Database, parse_model=False)
@pass_displayer
@pass_ctx
def database_alter(
    ctx: Context, displayer: Displayer, instance: Instance, name: str, **changes: Any
) -> None:
    """Alter a database in a PostgreSQL instance"""
    changes = helpers.unnest(interface.Database, changes)
    with instance_mod.running(ctx, instance):
        values = databases.describe(ctx, instance, name).dict()
        values = deep_update(values, changes)
        altered = interface.Database.parse_obj(values)
        with displayer:
            databases.apply(ctx, instance, altered)


@database.command("schema")
def database_schema() -> None:
    """Print the JSON schema of database model"""
    console.print_json(interface.Database.schema_json(indent=2))


@database.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_displayer
@pass_ctx
def database_apply(
    ctx: Context, displayer: Displayer, instance: Instance, file: IO[str]
) -> None:
    """Apply manifest as a database"""
    database = interface.Database.parse_yaml(file)
    with displayer, instance_mod.running(ctx, instance):
        databases.apply(ctx, instance, database)


@database.command("describe")
@instance_identifier
@click.argument("name")
@pass_ctx
def database_describe(ctx: Context, instance: Instance, name: str) -> None:
    """Describe a database"""
    with instance_mod.running(ctx, instance):
        described = databases.describe(ctx, instance, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@database.command("list")
@instance_identifier
@as_json_option
@pass_ctx
def database_list(ctx: Context, instance: Instance, as_json: bool) -> None:
    """List databases"""
    with instance_mod.running(ctx, instance):
        dbs = databases.list(ctx, instance)
    if as_json:
        print_json_for(dbs)
    else:
        print_table_for(dbs)


@database.command("drop")
@instance_identifier
@click.argument("name")
@pass_displayer
@pass_ctx
def database_drop(
    ctx: Context, displayer: Displayer, instance: Instance, name: str
) -> None:
    """Drop a database"""
    with instance_mod.running(ctx, instance), displayer:
        databases.drop(ctx, instance, name)


@database.command("privileges")
@instance_identifier
@click.argument("name")
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@as_json_option
@pass_ctx
def database_privileges(
    ctx: Context, instance: Instance, name: str, roles: Sequence[str], as_json: bool
) -> None:
    """List default privileges on a database."""
    with instance_mod.running(ctx, instance):
        databases.describe(ctx, instance, name)  # check existence
        try:
            prvlgs = privileges.get(ctx, instance, databases=(name,), roles=roles)
        except ValueError as e:
            raise click.ClickException(str(e))
    if as_json:
        print_json_for(prvlgs)
    else:
        print_table_for(prvlgs)


@database.command("run")
@instance_identifier
@click.argument("sql_command")
@click.option(
    "-d", "--database", "dbnames", multiple=True, help="Database to run command on"
)
@click.option(
    "-x",
    "--exclude-database",
    "exclude_dbnames",
    multiple=True,
    help="Database to not run command on",
)
@pass_ctx
def database_run(
    ctx: Context,
    instance: Instance,
    sql_command: str,
    dbnames: Sequence[str],
    exclude_dbnames: Sequence[str],
) -> None:
    """Run given command on databases of a PostgreSQL instance"""
    with instance_mod.running(ctx, instance):
        databases.run(
            ctx, instance, sql_command, dbnames=dbnames, exclude_dbnames=exclude_dbnames
        )


@cli.group("postgres_exporter")
@pass_ctx
@require_prometheus
def postgres_exporter(ctx: Context) -> None:
    """Handle Prometheus postgres_exporter"""


@postgres_exporter.command("schema")
def postgres_exporter_schema() -> None:
    """Print the JSON schema of database model"""
    console.print_json(interface.PostgresExporter.schema_json(indent=2))


@postgres_exporter.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_displayer
@pass_ctx
def postgres_exporter_apply(ctx: Context, displayer: Displayer, file: IO[str]) -> None:
    """Apply manifest as a Prometheus postgres_exporter."""
    exporter = interface.PostgresExporter.parse_yaml(file)
    with displayer:
        prometheus.apply(ctx, exporter)


@postgres_exporter.command("install")
@helpers.parameters_from_model(interface.PostgresExporter)
@pass_displayer
@pass_ctx
def postgres_exporter_install(
    ctx: Context, displayer: Displayer, postgresexporter: interface.PostgresExporter
) -> None:
    """Install the service for a (non-local) instance."""
    with displayer, task.transaction():
        prometheus.apply(ctx, postgresexporter)


@postgres_exporter.command("uninstall")
@click.argument("name")
@pass_displayer
@pass_ctx
def postgres_exporter_uninstall(ctx: Context, displayer: Displayer, name: str) -> None:
    """Uninstall the service."""
    with displayer:
        prometheus.drop(ctx, name)


@postgres_exporter.command("start")
@click.argument("name")
@foreground_option
@pass_displayer
@pass_ctx
def postgres_exporter_start(
    ctx: Context, displayer: Displayer, name: str, foreground: bool
) -> None:
    """Start postgres_exporter service NAME.

    The NAME argument is a local identifier for the postgres_exporter
    service. If the service is bound to a local instance, it should be
    <version>-<name>.
    """
    with displayer:
        prometheus.start(ctx, name, foreground=foreground)


@postgres_exporter.command("stop")
@click.argument("name")
@pass_displayer
@pass_ctx
def postgres_exporter_stop(ctx: Context, displayer: Displayer, name: str) -> None:
    """Stop postgres_exporter service NAME.

    The NAME argument is a local identifier for the postgres_exporter
    service. If the service is bound to a local instance, it should be
    <version>-<name>.
    """
    with displayer:
        prometheus.stop(ctx, name)
