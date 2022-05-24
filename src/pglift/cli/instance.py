from typing import IO, Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

import click
from pydantic.utils import deep_update
from rich.console import Console

from .. import instances, privileges, task
from ..ctx import Context
from ..instances import Status
from ..models import helpers, interface, system
from ..settings import PostgreSQLVersion
from .util import (
    Command,
    Group,
    _list_instances,
    as_json_option,
    foreground_option,
    instance_lookup,
    pass_console,
    pass_ctx,
    print_json_for,
    print_schema,
    print_table_for,
)

Callback = Callable[..., Any]
CommandFactory = Callable[[Type[interface.Instance]], Callback]


def instance_identifier(nargs: int = 1) -> Callable[[Callback], Callback]:
    def decorator(fn: Callback) -> Callback:
        command = click.argument(
            "instance",
            nargs=nargs,
            required=False,
            callback=instance_lookup,
            shell_complete=_list_instances,
        )(fn)
        assert command.__doc__
        command.__doc__ += (
            "\n\nINSTANCE identifies target instance as <version>/<name> where the "
            "<version>/ prefix may be omitted if there is only one instance "
            "matching <name>. Required if there is more than one instance on "
            "system."
        )
        return command

    return decorator


class InstanceCommands(Group):
    """Group for 'instance' sub-commands handling some of them that require a
    composite interface.Instance model built from registered plugins at
    runtime.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._composite_instance_commands: Dict[str, CommandFactory] = {}
        self._plugin_commands_loaded = False

    def command_with_composite_instance(
        self, name: str
    ) -> Callable[[CommandFactory], None]:
        """Decorator for callback that needs a composite Instance model"""
        assert name not in self._composite_instance_commands, name

        def decorator(factory: CommandFactory) -> None:
            self._composite_instance_commands[name] = factory

        return decorator

    def _load_plugins_commands(self, context: click.Context) -> bool:
        if self._plugin_commands_loaded:
            return False

        obj = context.obj
        if obj is None:
            from . import InvalidSettingsError, Obj

            try:
                obj = context.ensure_object(Obj)
            except InvalidSettingsError:
                return False

        if obj is None:
            return False

        obj.ctx.hook.instance_cli(group=self)
        self._plugin_commands_loaded = True
        return True

    def add_command(self, cmd: click.Command, name: Optional[str] = None) -> None:
        name = name or cmd.name
        assert (
            name not in self.commands
        ), f"instance command '{name}' already registered"
        super().add_command(cmd, name)

    def list_commands(self, context: click.Context) -> List[str]:
        self._load_plugins_commands(context)
        return sorted(
            super().list_commands(context) + list(self._composite_instance_commands)
        )

    def get_command(self, context: click.Context, name: str) -> Optional[click.Command]:
        self._load_plugins_commands(context)
        try:
            factory = self._composite_instance_commands[name]
        except KeyError:
            return super().get_command(context, name)
        else:
            if context.obj is None:  # During shell complete.
                model = interface.Instance
            else:
                model = interface.Instance.composite(context.obj.ctx.pm)
            f = factory(model)
            return click.command(cls=Command)(f)


def print_instance_schema(
    context: click.Context, param: click.Parameter, value: bool
) -> None:
    return print_schema(
        context, param, value, model=interface.Instance.composite(context.obj.ctx.pm)
    )


@click.group(cls=InstanceCommands)
@click.option(
    "--schema",
    is_flag=True,
    callback=print_instance_schema,
    expose_value=False,
    is_eager=True,
    help="Print the JSON schema of instance model and exit.",
)
def cli() -> None:
    """Manage instances."""


# Help mypy because click.group() looses the type of 'cls' argument.
assert isinstance(cli, InstanceCommands)


@cli.command_with_composite_instance("create")
def _instance_create(
    composite_instance_model: Type[interface.Instance],
) -> Callback:
    @helpers.parameters_from_model(composite_instance_model, "create")
    @pass_ctx
    def command(ctx: Context, instance: interface.Instance) -> None:
        """Initialize a PostgreSQL instance"""
        if instances.exists(ctx, instance.name, instance.version):
            raise click.ClickException("instance already exists")
        with task.transaction():
            instances.apply(ctx, instance)

    return command


@cli.command_with_composite_instance("alter")
def _instance_alter(
    composite_instance_model: Type[interface.Instance],
) -> Callback:
    @instance_identifier(nargs=1)
    @helpers.parameters_from_model(
        composite_instance_model, "update", parse_model=False
    )
    @pass_ctx
    def command(ctx: Context, instance: system.Instance, **changes: Any) -> None:
        """Alter PostgreSQL INSTANCE"""
        changes = helpers.unnest(composite_instance_model, changes)
        values = instances._get(ctx, instance).dict(exclude={"configuration"})
        values = deep_update(values, changes)
        altered = composite_instance_model.parse_obj(values)
        instances.apply(ctx, altered)

    return command


@cli.command("apply", hidden=True)
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_ctx
def instance_apply(ctx: Context, file: IO[str]) -> None:
    """Apply manifest as a PostgreSQL instance"""
    model = interface.Instance.composite(ctx.pm)
    instance = model.parse_yaml(file)
    instances.apply(ctx, instance)


@cli.command("promote")
@instance_identifier(nargs=1)
@pass_ctx
def instance_promote(ctx: Context, instance: system.Instance) -> None:
    """Promote standby PostgreSQL INSTANCE"""
    instances.promote(ctx, instance)


@cli.command("get")
@as_json_option
@instance_identifier(nargs=1)
@pass_console
@pass_ctx
def instance_get(
    ctx: Context, console: Console, instance: system.Instance, as_json: bool
) -> None:
    """Get the description of PostgreSQL INSTANCE.

    Unless --json is specified, 'configuration' and 'state' fields are not
    shown as well as 'standby' information if INSTANCE is not a standby.
    """
    exclude = set()
    if not as_json:
        exclude.update(["configuration", "state", "powa"])
        if not instance.standby:
            exclude.add("standby")
    m = instances.get(ctx, instance.name, instance.version).dict(
        by_alias=True, exclude=exclude
    )
    if as_json:
        print_json_for(m, display=console.print_json)
    else:
        print_table_for([m], display=console.print, box=None)


@cli.command("list")
@click.option(
    "--version",
    type=click.Choice(list(PostgreSQLVersion)),
    help="Only list instances of specified version.",
)
@as_json_option
@pass_console
@pass_ctx
def instance_list(
    ctx: Context, console: Console, version: Optional[PostgreSQLVersion], as_json: bool
) -> None:
    """List the available instances"""

    insts = instances.list(ctx, version=version)
    if as_json:
        print_json_for(
            (i.dict(by_alias=True) for i in insts), display=console.print_json
        )
    else:
        print_table_for((i.dict(by_alias=True) for i in insts), display=console.print)


@cli.command("drop")
@instance_identifier(nargs=-1)
@pass_ctx
def instance_drop(ctx: Context, instance: Tuple[system.Instance, ...]) -> None:
    """Drop PostgreSQL INSTANCE"""
    for i in instance:
        instances.drop(ctx, i)


@cli.command("status")
@instance_identifier(nargs=1)
@click.pass_context
def instance_status(context: click.Context, instance: system.Instance) -> None:
    """Check the status of PostgreSQL INSTANCE.

    Output the status string value ('running', 'not running', 'unspecified
    datadir') and exit with respective status code (0, 3, 4).
    """
    ctx = context.obj.ctx
    status = instances.status(ctx, instance)
    click.echo(status.name.replace("_", " "))
    context.exit(status.value)


@cli.command("start")
@instance_identifier(nargs=-1)
@foreground_option
@pass_ctx
def instance_start(
    ctx: Context, instance: Tuple[system.Instance, ...], foreground: bool
) -> None:
    """Start PostgreSQL INSTANCE"""
    if foreground and len(instance) != 1:
        raise click.UsageError(
            "only one INSTANCE argument may be given with --foreground"
        )
    for i in instance:
        instances.check_status(ctx, i, Status.not_running)
        instances.start(ctx, i, foreground=foreground)


@cli.command("stop")
@instance_identifier(nargs=-1)
@pass_ctx
def instance_stop(ctx: Context, instance: Tuple[system.Instance, ...]) -> None:
    """Stop PostgreSQL INSTANCE"""
    for i in instance:
        instances.stop(ctx, i)


@cli.command("reload")
@instance_identifier(nargs=-1)
@pass_ctx
def instance_reload(ctx: Context, instance: Tuple[system.Instance, ...]) -> None:
    """Reload PostgreSQL INSTANCE"""
    for i in instance:
        instances.reload(ctx, i)


@cli.command("restart")
@instance_identifier(nargs=-1)
@pass_ctx
def instance_restart(ctx: Context, instance: Tuple[system.Instance, ...]) -> None:
    """Restart PostgreSQL INSTANCE"""
    for i in instance:
        instances.restart(ctx, i)


@cli.command("exec")
@instance_identifier(nargs=1)
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
@pass_ctx
def instance_exec(
    ctx: Context, instance: system.Instance, command: Tuple[str, ...]
) -> None:
    """Execute command in the libpq environment for PostgreSQL INSTANCE"""
    if not command:
        raise click.ClickException("no command given")
    instances.exec(ctx, instance, command)


@cli.command("env")
@instance_identifier(nargs=1)
@pass_ctx
def instance_env(ctx: Context, instance: system.Instance) -> None:
    """Output environment variables suitable to handle to PostgreSQL INSTANCE.

    This can be injected in shell using:

    export $(pglift instance env myinstance)
    """
    for key, value in sorted(instances.env_for(ctx, instance, path=True).items()):
        click.echo(f"{key}={value}")


@cli.command("logs")
@instance_identifier(nargs=1)
@pass_ctx
def instance_logs(ctx: Context, instance: system.Instance) -> None:
    """Output INSTANCE logs

    This assumes that the PostgreSQL instance is configured to use file-based
    logging (i.e. log_destination amongst 'stderr' or 'csvlog').
    """
    for line in instances.logs(ctx, instance):
        click.echo(line, nl=False)


@cli.command("privileges")
@instance_identifier(nargs=1)
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@click.option("--default", "defaults", is_flag=True, help="Display default privileges")
@as_json_option
@pass_console
@pass_ctx
def instance_privileges(
    ctx: Context,
    console: Console,
    instance: system.Instance,
    databases: Sequence[str],
    roles: Sequence[str],
    defaults: bool,
    as_json: bool,
) -> None:
    """List privileges on INSTANCE"""
    with instances.running(ctx, instance):
        try:
            prvlgs = privileges.get(
                ctx, instance, databases=databases, roles=roles, defaults=defaults
            )
        except ValueError as e:
            raise click.ClickException(str(e))
    if as_json:
        print_json_for(
            (i.dict(by_alias=True) for i in prvlgs), display=console.print_json
        )
    else:
        if defaults:
            title = f"Default privileges on instance {instance}"
        else:
            title = f"Privileges on instance {instance}"
        print_table_for(
            (i.dict(by_alias=True) for i in prvlgs),
            title=title,
            display=console.print,
        )


@cli.command("upgrade")
@instance_identifier(nargs=1)
@click.option(
    "--version",
    "newversion",
    type=click.Choice(list(PostgreSQLVersion)),
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
    newversion: Optional[PostgreSQLVersion],
    newname: Optional[str],
    port: Optional[int],
    jobs: Optional[int],
) -> None:
    """Upgrade INSTANCE using pg_upgrade"""
    instances.check_status(ctx, instance, Status.not_running)
    new_instance = instances.upgrade(
        ctx, instance, version=newversion, name=newname, port=port, jobs=jobs
    )
    instances.start(ctx, new_instance)
