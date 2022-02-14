import json
import logging
import os
import pathlib
import tempfile
import time
from datetime import datetime
from functools import partial, wraps
from types import ModuleType
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
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
from click.shell_completion import CompletionItem
from pydantic.utils import deep_update
from rich.console import Console, RenderableType
from rich.highlighter import NullHighlighter
from rich.table import Table
from typing_extensions import Literal

from . import __name__ as pkgname
from . import _install, conf, databases, exceptions
from . import instance as instance_mod
from . import pgbackrest as pgbackrest_mod
from . import privileges, prometheus, roles, task, version
from .ctx import Context
from .instance import Status
from .models import helpers, interface, system
from .settings import (
    POSTGRESQL_SUPPORTED_VERSIONS,
    PgBackRestSettings,
    PrometheusSettings,
    Settings,
)
from .task import Displayer
from .types import ConfigChanges

logger = logging.getLogger(__name__)
CONSOLE = Console()

Callback = Callable[..., Any]


class LogDisplayer:
    def handle(self, msg: str) -> None:
        logger.info(msg)


class CLIContext(Context):
    def confirm(self, message: str, default: bool) -> bool:
        return rich.prompt.Confirm(console=CONSOLE).ask(f"[yellow]>[/yellow] {message}")


class Obj:
    """Object bound to click.Context"""

    def __init__(self, context: CLIContext, displayer: Optional[Displayer]) -> None:
        self.ctx = context
        self.displayer = displayer


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
            except exceptions.Cancelled as e:
                logger.warning(str(e))
                raise click.Abort
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


def pass_ctx(f: C) -> C:
    """Command decorator passing 'Context' bound to click.Context's object."""

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        context = click.get_current_context()
        ctx = context.obj.ctx
        assert isinstance(ctx, Context), ctx
        return context.invoke(f, ctx, *args, **kwargs)

    return cast(C, wrapper)


def pass_component_settings(mod: ModuleType, name: str, f: C) -> C:
    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        context = click.get_current_context()
        ctx = context.obj.ctx
        assert isinstance(ctx, Context), ctx
        settings = getattr(mod, "available")(ctx)
        if not settings:
            click.echo(f"{name} not available", err=True)
            raise Exit(1)
        context.invoke(f, settings, *args, **kwargs)

    return cast(C, wrapper)


pass_pgbackrest_settings = partial(
    pass_component_settings, pgbackrest_mod, "pgbackrest"
)
pass_prometheus_settings = partial(
    pass_component_settings, prometheus, "Prometheus postgres_exporter"
)


def get_instance(ctx: Context, name: str, version: Optional[str]) -> system.Instance:
    """Return an Instance from name/version, possibly guessing version if unspecified."""
    if version is None:
        found = None
        for version in POSTGRESQL_SUPPORTED_VERSIONS:
            try:
                instance = system.Instance.system_lookup(ctx, (name, version))
            except exceptions.InstanceNotFound:
                logger.debug("instance '%s' not found in version %s", name, version)
            else:
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
        return system.Instance.system_lookup(ctx, (name, version))
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
) -> system.Instance:
    name, version = nameversion_from_id(value)
    ctx = context.obj.ctx
    return get_instance(ctx, name, version)


def _list_instances(
    context: click.Context, param: click.Parameter, incomplete: str
) -> List[CompletionItem]:
    """Shell completion function for instance identifier <name> or <version>/<name>."""
    out = []
    iname, iversion = nameversion_from_id(incomplete)
    ctx = Context(settings=Settings())
    for i in instance_mod.list(ctx):
        if iversion is not None and i.version.startswith(iversion):
            if i.name.startswith(iname):
                out.append(
                    CompletionItem(f"{i.version}/{i.name}", help=f"port={i.port}")
                )
            else:
                out.append(CompletionItem(i.version))
        else:
            out.append(
                CompletionItem(i.name, help=f"{i.version}/{i.name} port={i.port}")
            )
    return out


instance_identifier = click.argument(
    "instance",
    metavar="<version>/<name>",
    callback=instance_lookup,
    shell_complete=_list_instances,
)


_M = TypeVar("_M", bound=pydantic.BaseModel)


def print_table_for(
    items: Iterable[_M],
    title: Optional[str] = None,
    *,
    display: Callable[[RenderableType], None] = CONSOLE.print,
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
    items: Iterable[_M], *, display: Callable[[str], None] = CONSOLE.print_json
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
        displayer = None if log_file else LogDisplayer()
        context.obj = Obj(CLIContext(settings=Settings()), displayer)
    else:
        assert isinstance(context.obj, Obj), context.obj


@cli.command("site-settings", hidden=True)
@pass_ctx
def site_settings(ctx: Context) -> None:
    """Show site settings."""
    CONSOLE.print_json(ctx.settings.json())


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


CommandFactory = Callable[[Type[interface.Instance]], Callback]


class CompositeInstanceCommands(click.MultiCommand):
    """MultiCommand for 'instance' sub-commands that require a composite
    interface.Instance model built from registered plugins at runtime.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._instance_commands: Dict[str, CommandFactory] = {}

    def register(self, name: str) -> Callable[[CommandFactory], None]:
        assert name not in self._instance_commands, name

        def decorator(factory: CommandFactory) -> None:
            self._instance_commands[name] = factory

        return decorator

    def list_commands(self, context: click.Context) -> List[str]:
        return sorted(self._instance_commands)

    def get_command(self, context: click.Context, name: str) -> Optional[click.Command]:
        try:
            factory = self._instance_commands[name]
        except KeyError:
            return None
        else:
            composite_instance_model = interface.Instance.composite(context.obj.ctx.pm)
            f = factory(composite_instance_model)
            return click.command(cls=Command)(f)


composite_instance_commands = CompositeInstanceCommands()


@composite_instance_commands.register("create")
def _instance_create(
    composite_instance_model: Type[interface.Instance],
) -> Callback:
    @helpers.parameters_from_model(composite_instance_model)
    @pass_ctx
    def command(ctx: Context, instance: interface.Instance) -> None:
        """Initialize a PostgreSQL instance"""
        if instance_mod.exists(ctx, instance.name, instance.version):
            raise click.ClickException("instance already exists")
        with task.transaction():
            instance_mod.apply(ctx, instance)

    return command


@composite_instance_commands.register("alter")
def _instance_alter(
    composite_instance_model: Type[interface.Instance],
) -> Callback:
    @instance_identifier
    @helpers.parameters_from_model(
        composite_instance_model, exclude=["name", "version"], parse_model=False
    )
    @pass_ctx
    def command(ctx: Context, instance: system.Instance, **changes: Any) -> None:
        """Alter a PostgreSQL instance"""
        changes = helpers.unnest(composite_instance_model, changes)
        values = instance_mod.describe(ctx, instance.name, instance.version).dict()
        values = deep_update(values, changes)
        altered = composite_instance_model.parse_obj(values)
        instance_mod.apply(ctx, altered)

    return command


@composite_instance_commands.register("schema")
def _instance_schema(
    composite_instance_model: Type[interface.Instance],
) -> Callback:
    def command() -> None:
        """Print the JSON schema of PostgreSQL instance model"""
        CONSOLE.print_json(composite_instance_model.schema_json(indent=2))

    return command


@cli.group()
def instance() -> None:
    """Manipulate instances"""


cli.add_command(
    click.CommandCollection(sources=[instance, composite_instance_commands]),
    name="instance",
)


@instance.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_ctx
def instance_apply(ctx: Context, file: IO[str]) -> None:
    """Apply manifest as a PostgreSQL instance"""
    instance = interface.Instance.parse_yaml(file)
    instance_mod.apply(ctx, instance)


@instance.command("promote")
@instance_identifier
@pass_ctx
def instance_promote(ctx: Context, instance: system.Instance) -> None:
    """Promote a standby PostgreSQL instance"""
    instance_mod.promote(ctx, instance)


@instance.command("describe")
@instance_identifier
@pass_ctx
def instance_describe(ctx: Context, instance: system.Instance) -> None:
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
@pass_ctx
def instance_list(ctx: Context, version: Optional[str], as_json: bool) -> None:
    """List the available instances"""

    instances = instance_mod.list(ctx, version=version)
    if as_json:
        print_json_for(instances)
    else:
        print_table_for(instances)


@instance.command("drop")
@instance_identifier
@pass_ctx
def instance_drop(ctx: Context, instance: system.Instance) -> None:
    """Drop a PostgreSQL instance"""
    instance_mod.drop(ctx, instance)


@instance.command("status")
@instance_identifier
@click.pass_context
def instance_status(context: click.Context, instance: system.Instance) -> None:
    """Check the status of a PostgreSQL instance.

    Output the status string value ('running', 'not running', 'unspecified
    datadir') and exit with respective status code (0, 3, 4).
    """
    ctx = context.obj.ctx
    status = instance_mod.status(ctx, instance)
    click.echo(status.name.replace("_", " "))
    context.exit(status.value)


@instance.command("start")
@instance_identifier
@foreground_option
@pass_ctx
def instance_start(ctx: Context, instance: system.Instance, foreground: bool) -> None:
    """Start a PostgreSQL instance"""
    instance_mod.check_status(ctx, instance, Status.not_running)
    instance_mod.start(ctx, instance, foreground=foreground)


@instance.command("stop")
@instance_identifier
@pass_ctx
def instance_stop(ctx: Context, instance: system.Instance) -> None:
    """Stop a PostgreSQL instance"""
    instance_mod.stop(ctx, instance)


@instance.command("reload")
@instance_identifier
@pass_ctx
def instance_reload(ctx: Context, instance: system.Instance) -> None:
    """Reload a PostgreSQL instance"""
    instance_mod.reload(ctx, instance)


@instance.command("restart")
@instance_identifier
@pass_ctx
def instance_restart(ctx: Context, instance: system.Instance) -> None:
    """Restart a PostgreSQL instance"""
    instance_mod.restart(ctx, instance)


@instance.command("exec")
@instance_identifier
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
@pass_ctx
def instance_exec(
    ctx: Context, instance: system.Instance, command: Tuple[str, ...]
) -> None:
    """Execute command in the libpq environment for a PostgreSQL instance."""
    if not command:
        raise click.ClickException("no command given")
    instance_mod.exec(ctx, instance, command)


@instance.command("env")
@instance_identifier
@pass_ctx
def instance_env(ctx: Context, instance: system.Instance) -> None:
    """Output environment variables suitable to connect to a PostgreSQL instance.

    This can be injected in shell using:

    export $(pglift instance env myinstance)
    """
    for key, value in sorted(instance_mod.env_for(ctx, instance, path=True).items()):
        click.echo(f"{key}={value}")


@instance.command("logs")
@instance_identifier
@pass_ctx
def instance_logs(ctx: Context, instance: system.Instance) -> None:
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
    type=click.Choice([t.name for t in pgbackrest_mod.BackupType]),
    default=pgbackrest_mod.BackupType.default().name,
    help="Backup type",
    callback=lambda ctx, param, value: pgbackrest_mod.BackupType(value),
)
@pass_pgbackrest_settings
@pass_ctx
def instance_backup(
    ctx: Context,
    settings: PgBackRestSettings,
    instance: system.Instance,
    backup_type: pgbackrest_mod.BackupType,
) -> None:
    """Back up a PostgreSQL instance"""
    pgbackrest_mod.backup(ctx, instance, settings, type=backup_type)


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
@pass_pgbackrest_settings
@pass_ctx
def instance_restore(
    ctx: Context,
    settings: PgBackRestSettings,
    instance: system.Instance,
    list_only: bool,
    label: Optional[str],
    date: Optional[datetime],
) -> None:
    """Restore a PostgreSQL instance"""
    if list_only:
        backups = pgbackrest_mod.iter_backups(ctx, instance, settings)
        print_table_for(backups, title=f"Available backups for instance {instance}")
    else:
        instance_mod.check_status(ctx, instance, Status.not_running)
        if label is not None and date is not None:
            raise click.BadArgumentUsage(
                "--label and --date arguments are mutually exclusive"
            )
        pgbackrest_mod.restore(ctx, instance, settings, label=label, date=date)


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
    instance: system.Instance,
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
@click.option(
    "--version",
    "newversion",
    help="PostgreSQL version of the new instance (default to site-configured value).",
)
@click.option(
    "--name", "newname", help="Name of the new instance (default to old instance name)."
)
@click.option(
    "--port", required=False, type=click.INT, help="Port of the new instance."
)
@click.option(
    "--jobs",
    required=False,
    type=click.INT,
    help="Number of simultaneous processes or threads to use (from pg_upgrade).",
)
@pass_ctx
def instance_upgrade(
    ctx: Context,
    instance: system.Instance,
    newversion: Optional[str],
    newname: Optional[str],
    port: Optional[int],
    jobs: Optional[int],
) -> None:
    """Upgrade an instance using pg_upgrade and configure respective satellite components"""
    instance_mod.check_status(ctx, instance, Status.not_running)
    new_instance = instance_mod.upgrade(
        ctx, instance, version=newversion, name=newname, port=port, jobs=jobs
    )
    instance_mod.start(ctx, new_instance)


@cli.group("pgconf")
def pgconf() -> None:
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


@pgconf.command("show")
@instance_identifier
@click.argument("parameter", nargs=-1)
@pass_ctx
def pgconf_show(ctx: Context, instance: system.Instance, parameter: Tuple[str]) -> None:
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


@pgconf.command("set")
@instance_identifier
@click.argument(
    "parameters",
    metavar="<PARAMETER>=<VALUE>...",
    nargs=-1,
    callback=validate_configuration_parameters,
    required=True,
)
@pass_ctx
def pgconf_set(
    ctx: Context, instance: system.Instance, parameters: Dict[str, Any]
) -> None:
    """Set configuration items."""
    values = instance.config(managed_only=True).as_dict()
    values.update(parameters)
    manifest = interface.Instance(name=instance.name, version=instance.version)
    changes = instance_mod.configure(ctx, manifest, values=values)
    show_configuration_changes(changes, parameters.keys())


@pgconf.command("remove")
@instance_identifier
@click.argument("parameters", nargs=-1, required=True)
@pass_ctx
def pgconf_remove(
    ctx: Context, instance: system.Instance, parameters: Tuple[str]
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


@pgconf.command("edit")
@instance_identifier
@pass_ctx
def pgconf_edit(ctx: Context, instance: system.Instance) -> None:
    """Edit managed configuration."""
    confd = conf.info(instance.datadir)[0]
    click.edit(filename=str(confd / "user.conf"))


@cli.group("role")
def role() -> None:
    """Manipulate roles"""


@role.command("create")
@instance_identifier
@helpers.parameters_from_model(interface.Role)
@pass_ctx
def role_create(ctx: Context, instance: system.Instance, role: interface.Role) -> None:
    """Create a role in a PostgreSQL instance"""
    with instance_mod.running(ctx, instance):
        if roles.exists(ctx, instance, role.name):
            raise click.ClickException("role already exists")
        with task.transaction():
            roles.apply(ctx, instance, role)


@role.command("alter")
@instance_identifier
@helpers.parameters_from_model(interface.Role, parse_model=False)
@pass_ctx
def role_alter(
    ctx: Context, instance: system.Instance, name: str, **changes: Any
) -> None:
    """Alter a role in a PostgreSQL instance"""
    changes = helpers.unnest(interface.Role, changes)
    with instance_mod.running(ctx, instance):
        values = roles.describe(ctx, instance, name).dict()
        values = deep_update(values, changes)
        altered = interface.Role.parse_obj(values)
        roles.apply(ctx, instance, altered)


@role.command("schema")
def role_schema() -> None:
    """Print the JSON schema of role model"""
    CONSOLE.print_json(interface.Role.schema_json(indent=2))


@role.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_ctx
def role_apply(ctx: Context, instance: system.Instance, file: IO[str]) -> None:
    """Apply manifest as a role"""
    role = interface.Role.parse_yaml(file)
    with instance_mod.running(ctx, instance):
        roles.apply(ctx, instance, role)


@role.command("describe")
@instance_identifier
@click.argument("name")
@pass_ctx
def role_describe(ctx: Context, instance: system.Instance, name: str) -> None:
    """Describe a role"""
    with instance_mod.running(ctx, instance):
        described = roles.describe(ctx, instance, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@role.command("drop")
@instance_identifier
@click.argument("name")
@pass_ctx
def role_drop(ctx: Context, instance: system.Instance, name: str) -> None:
    """Drop a role"""
    with instance_mod.running(ctx, instance):
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
    ctx: Context,
    instance: system.Instance,
    name: str,
    databases: Sequence[str],
    as_json: bool,
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
@pass_ctx
def database_create(
    ctx: Context, instance: system.Instance, database: interface.Database
) -> None:
    """Create a database in a PostgreSQL instance"""
    with instance_mod.running(ctx, instance):
        if databases.exists(ctx, instance, database.name):
            raise click.ClickException("database already exists")
        with task.transaction():
            databases.apply(ctx, instance, database)


@database.command("alter")
@instance_identifier
@helpers.parameters_from_model(interface.Database, parse_model=False)
@pass_ctx
def database_alter(
    ctx: Context, instance: system.Instance, name: str, **changes: Any
) -> None:
    """Alter a database in a PostgreSQL instance"""
    changes = helpers.unnest(interface.Database, changes)
    with instance_mod.running(ctx, instance):
        values = databases.describe(ctx, instance, name).dict()
        values = deep_update(values, changes)
        altered = interface.Database.parse_obj(values)
        databases.apply(ctx, instance, altered)


@database.command("schema")
def database_schema() -> None:
    """Print the JSON schema of database model"""
    CONSOLE.print_json(interface.Database.schema_json(indent=2))


@database.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_ctx
def database_apply(ctx: Context, instance: system.Instance, file: IO[str]) -> None:
    """Apply manifest as a database"""
    database = interface.Database.parse_yaml(file)
    with instance_mod.running(ctx, instance):
        databases.apply(ctx, instance, database)


@database.command("describe")
@instance_identifier
@click.argument("name")
@pass_ctx
def database_describe(ctx: Context, instance: system.Instance, name: str) -> None:
    """Describe a database"""
    with instance_mod.running(ctx, instance):
        described = databases.describe(ctx, instance, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@database.command("list")
@instance_identifier
@as_json_option
@pass_ctx
def database_list(ctx: Context, instance: system.Instance, as_json: bool) -> None:
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
@pass_ctx
def database_drop(ctx: Context, instance: system.Instance, name: str) -> None:
    """Drop a database"""
    with instance_mod.running(ctx, instance):
        databases.drop(ctx, instance, name)


@database.command("privileges")
@instance_identifier
@click.argument("name")
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@as_json_option
@pass_ctx
def database_privileges(
    ctx: Context,
    instance: system.Instance,
    name: str,
    roles: Sequence[str],
    as_json: bool,
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
    instance: system.Instance,
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
def postgres_exporter(ctx: Context) -> None:
    """Handle Prometheus postgres_exporter"""


@postgres_exporter.command("schema")
def postgres_exporter_schema() -> None:
    """Print the JSON schema of database model"""
    CONSOLE.print_json(prometheus.PostgresExporter.schema_json(indent=2))


@postgres_exporter.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_apply(
    ctx: Context, settings: PrometheusSettings, file: IO[str]
) -> None:
    """Apply manifest as a Prometheus postgres_exporter."""
    exporter = prometheus.PostgresExporter.parse_yaml(file)
    prometheus.apply(ctx, exporter, settings)


@postgres_exporter.command("install")
@helpers.parameters_from_model(prometheus.PostgresExporter)
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_install(
    ctx: Context,
    settings: PrometheusSettings,
    postgresexporter: prometheus.PostgresExporter,
) -> None:
    """Install the service for a (non-local) instance."""
    with task.transaction():
        prometheus.apply(ctx, postgresexporter, settings)


@postgres_exporter.command("uninstall")
@click.argument("name")
@pass_ctx
def postgres_exporter_uninstall(ctx: Context, name: str) -> None:
    """Uninstall the service."""
    prometheus.drop(ctx, name)


@postgres_exporter.command("start")
@click.argument("name")
@foreground_option
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_start(
    ctx: Context, settings: PrometheusSettings, name: str, foreground: bool
) -> None:
    """Start postgres_exporter service NAME.

    The NAME argument is a local identifier for the postgres_exporter
    service. If the service is bound to a local instance, it should be
    <version>-<name>.
    """
    prometheus.start(ctx, name, settings, foreground=foreground)


@postgres_exporter.command("stop")
@click.argument("name")
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_stop(
    ctx: Context, settings: PrometheusSettings, name: str
) -> None:
    """Stop postgres_exporter service NAME.

    The NAME argument is a local identifier for the postgres_exporter
    service. If the service is bound to a local instance, it should be
    <version>-<name>.
    """
    prometheus.stop(ctx, name, settings)


@cli.command("pgbackrest", hidden=True)
@instance_identifier
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
@pass_pgbackrest_settings
@pass_ctx
def pgbackrest(
    ctx: Context,
    settings: PgBackRestSettings,
    instance: system.Instance,
    command: Tuple[str, ...],
) -> None:
    """Proxy to pgbackrest operations on an instance"""
    cmd = pgbackrest_mod.make_cmd(instance, settings, *command)
    ctx.run(cmd, redirect_output=True, check=True)
