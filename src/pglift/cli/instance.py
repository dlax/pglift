from datetime import datetime
from functools import partial
from typing import IO, Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

import click
from pydantic.utils import deep_update
from rich.console import Console

from .. import instance as instance_mod
from .. import privileges, task, types
from ..ctx import Context
from ..instance import Status
from ..models import helpers, interface, system
from ..pgbackrest import impl as pgbackrest_mod
from ..settings import POSTGRESQL_SUPPORTED_VERSIONS, PgBackRestSettings
from .util import (
    Command,
    Group,
    as_json_option,
    foreground_option,
    instance_identifier,
    pass_component_settings,
    pass_console,
    pass_ctx,
    print_json_for,
    print_table_for,
)

Callback = Callable[..., Any]
CommandFactory = Callable[[Type[interface.Instance]], Callback]


class InstanceCommands(Group):
    """Group for 'instance' sub-commands handling some of them that require a
    composite interface.Instance model built from registered plugins at
    runtime.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._composite_instance_commands: Dict[str, CommandFactory] = {}

    def command_with_composite_instance(
        self, name: str
    ) -> Callable[[CommandFactory], None]:
        """Decorator for callback that needs a composite Instance model."""
        assert name not in self._composite_instance_commands, name

        def decorator(factory: CommandFactory) -> None:
            self._composite_instance_commands[name] = factory

        return decorator

    def list_commands(self, context: click.Context) -> List[str]:
        return sorted(
            super().list_commands(context) + list(self._composite_instance_commands)
        )

    def get_command(self, context: click.Context, name: str) -> Optional[click.Command]:
        try:
            factory = self._composite_instance_commands[name]
        except KeyError:
            return super().get_command(context, name)
        else:
            composite_instance_model = interface.Instance.composite(context.obj.ctx.pm)
            f = factory(composite_instance_model)
            return click.command(cls=Command)(f)


@click.group(cls=InstanceCommands)
def cli() -> None:
    """Manage instances."""


# Help mypy because click.group() looses the type of 'cls' argument.
assert isinstance(cli, InstanceCommands)


@cli.command_with_composite_instance("create")
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


@cli.command_with_composite_instance("alter")
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


@cli.command_with_composite_instance("schema")
def _instance_schema(
    composite_instance_model: Type[interface.Instance],
) -> Callback:
    @pass_console
    def command(console: Console) -> None:
        """Print the JSON schema of PostgreSQL instance model"""
        console.print_json(composite_instance_model.schema_json(indent=2))

    return command


@cli.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_ctx
def instance_apply(ctx: Context, file: IO[str]) -> None:
    """Apply manifest as a PostgreSQL instance"""
    instance = interface.Instance.parse_yaml(file)
    instance_mod.apply(ctx, instance)


@cli.command("promote")
@instance_identifier
@pass_ctx
def instance_promote(ctx: Context, instance: system.Instance) -> None:
    """Promote a standby PostgreSQL instance"""
    instance_mod.promote(ctx, instance)


@cli.command("describe")
@instance_identifier
@pass_ctx
def instance_describe(ctx: Context, instance: system.Instance) -> None:
    """Describe a PostgreSQL instance"""
    described = instance_mod.describe(ctx, instance.name, instance.version)
    click.echo(described.yaml(), nl=False)


@cli.command("list")
@click.option(
    "--version",
    type=click.Choice(POSTGRESQL_SUPPORTED_VERSIONS),
    help="Only list instances of specified version.",
)
@as_json_option
@pass_console
@pass_ctx
def instance_list(
    ctx: Context, console: Console, version: Optional[str], as_json: bool
) -> None:
    """List the available instances"""

    instances = instance_mod.list(ctx, version=version)
    if as_json:
        print_json_for(instances, display=console.print_json)
    else:
        print_table_for(instances, display=console.print)


@cli.command("drop")
@instance_identifier
@pass_ctx
def instance_drop(ctx: Context, instance: system.Instance) -> None:
    """Drop a PostgreSQL instance"""
    instance_mod.drop(ctx, instance)


@cli.command("status")
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


@cli.command("start")
@instance_identifier
@foreground_option
@pass_ctx
def instance_start(ctx: Context, instance: system.Instance, foreground: bool) -> None:
    """Start a PostgreSQL instance"""
    instance_mod.check_status(ctx, instance, Status.not_running)
    instance_mod.start(ctx, instance, foreground=foreground)


@cli.command("stop")
@instance_identifier
@pass_ctx
def instance_stop(ctx: Context, instance: system.Instance) -> None:
    """Stop a PostgreSQL instance"""
    instance_mod.stop(ctx, instance)


@cli.command("reload")
@instance_identifier
@pass_ctx
def instance_reload(ctx: Context, instance: system.Instance) -> None:
    """Reload a PostgreSQL instance"""
    instance_mod.reload(ctx, instance)


@cli.command("restart")
@instance_identifier
@pass_ctx
def instance_restart(ctx: Context, instance: system.Instance) -> None:
    """Restart a PostgreSQL instance"""
    instance_mod.restart(ctx, instance)


@cli.command("exec")
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


@cli.command("env")
@instance_identifier
@pass_ctx
def instance_env(ctx: Context, instance: system.Instance) -> None:
    """Output environment variables suitable to connect to a PostgreSQL instance.

    This can be injected in shell using:

    export $(pglift instance env myinstance)
    """
    for key, value in sorted(instance_mod.env_for(ctx, instance, path=True).items()):
        click.echo(f"{key}={value}")


@cli.command("logs")
@instance_identifier
@pass_ctx
def instance_logs(ctx: Context, instance: system.Instance) -> None:
    """Output instance logs

    This assumes that the PostgreSQL instance is configured to use file-based
    logging (i.e. log_destination amongst 'stderr' or 'csvlog').
    """
    for line in instance_mod.logs(ctx, instance):
        click.echo(line, nl=False)


pass_pgbackrest_settings = partial(
    pass_component_settings, pgbackrest_mod, "pgbackrest"
)


@cli.command("backup")
@instance_identifier
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
    ctx: Context,
    settings: PgBackRestSettings,
    instance: system.Instance,
    backup_type: types.BackupType,
) -> None:
    """Back up a PostgreSQL instance"""
    pgbackrest_mod.backup(ctx, instance, settings, type=backup_type)


@cli.command("restore")
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
@pass_console
@pass_ctx
def instance_restore(
    ctx: Context,
    console: Console,
    settings: PgBackRestSettings,
    instance: system.Instance,
    list_only: bool,
    label: Optional[str],
    date: Optional[datetime],
) -> None:
    """Restore a PostgreSQL instance"""
    if list_only:
        backups = pgbackrest_mod.iter_backups(ctx, instance, settings)
        print_table_for(
            backups,
            title=f"Available backups for instance {instance}",
            display=console.print,
        )
    else:
        instance_mod.check_status(ctx, instance, Status.not_running)
        if label is not None and date is not None:
            raise click.BadArgumentUsage(
                "--label and --date arguments are mutually exclusive"
            )
        pgbackrest_mod.restore(ctx, instance, settings, label=label, date=date)


@cli.command("privileges")
@instance_identifier
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@as_json_option
@pass_console
@pass_ctx
def instance_privileges(
    ctx: Context,
    console: Console,
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
        print_json_for(prvlgs, display=console.print_json)
    else:
        print_table_for(
            prvlgs,
            title=f"Default privileges on instance {instance}",
            display=console.print,
        )


@cli.command("upgrade")
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
