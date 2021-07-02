import json
import logging
from typing import IO, Optional, Union

import click
import pydantic.json
from tabulate import tabulate
from typing_extensions import Literal

from . import _install, exceptions
from . import instance as instance_mod
from . import manifest, pgbackrest, pm, roles, uihelpers
from .ctx import Context
from .model import Instance, InstanceSpec
from .settings import SETTINGS
from .task import runner


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
)
@click.pass_context
def cli(ctx: click.core.Context, log_level: Optional[str]) -> None:
    """Deploy production-ready instances of PostgreSQL"""
    if log_level is not None:
        logging.basicConfig(level=log_level)

    if not ctx.obj:
        ctx.obj = Context(plugin_manager=pm.PluginManager.get(), settings=SETTINGS)


@cli.command(
    "site-configure",
    hidden=True,
    help="Manage installation of extra data files for pglift.\n\nThis is an INTERNAL command.",
)
@click.argument(
    "action", type=click.Choice(["install", "uninstall"]), default="install"
)
@click.option("--settings", type=click.Path(exists=True), help="custom settings file")
@click.pass_obj
def site_configure(
    ctx: Context,
    action: Union[Literal["install"], Literal["uninstall"]],
    settings: Optional[str],
) -> None:
    if action == "install":
        if settings:
            env = f"SETTINGS=@{settings}"
        _install.do(ctx, env=env)
    elif action == "uninstall":
        _install.undo(ctx)


@cli.group("instance")
def instance() -> None:
    """Manipulate instances"""


@instance.command("init")
@uihelpers.parameters_from_model(manifest.Instance)
@click.pass_obj
def instance_init(ctx: Context, m: manifest.Instance) -> None:
    """Initialize a PostgreSQL instance"""
    if m.model(ctx).exists():
        raise click.ClickException("instance already exists")
    with runner(ctx):
        instance_mod.apply(ctx, m)


@instance.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@click.pass_obj
def instance_apply(ctx: Context, file: IO[str]) -> None:
    """Apply manifest as a PostgreSQL instance"""
    with runner(ctx):
        instance_mod.apply(ctx, manifest.Instance.parse_yaml(file))


@instance.command("schema")
def instance_schema() -> None:
    """Print the JSON schema of PostgreSQL instance model"""
    print(manifest.Instance.schema_json(indent=2))


name_argument = click.argument("name", type=click.STRING)
version_argument = click.argument("version", required=False, type=click.STRING)


def get_instance(ctx: Context, name: str, version: Optional[str]) -> Instance:
    try:
        if version:
            spec = InstanceSpec(name, version, settings=ctx.settings)
        else:
            spec = InstanceSpec.default_version(name, ctx)
        return Instance.from_spec(spec)
    except exceptions.InstanceNotFound as e:
        raise click.ClickException(e.show())
    except Exception as e:
        raise click.ClickException(str(e))


def instance_lookup(ctx: Context, instance_id: str) -> Instance:
    version = None
    try:
        version, name = instance_id.split("/", 1)
    except ValueError:
        name = instance_id
    return get_instance(ctx, name, version)


@instance.command("describe")
@name_argument
@version_argument
@click.pass_obj
def instance_describe(ctx: Context, name: str, version: Optional[str]) -> None:
    """Describe a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    described = instance_mod.describe(ctx, instance)
    print(described.yaml(), end="")


@instance.command("list")
@click.option(
    "--version",
    type=click.Choice(list(SETTINGS.postgresql.versions)),
    help="Only list instances of specified version.",
)
@click.option("--json", "as_json", is_flag=True, help="Print as JSON")
@click.pass_obj
def instance_list(ctx: Context, version: Optional[str], as_json: bool) -> None:
    """List the available instances"""

    instances = instance_mod.list(ctx, version=version)
    if as_json:
        print(json.dumps(list(instances), default=pydantic.json.pydantic_encoder))
        return

    props = manifest.InstanceListItem.__fields__
    content = [[getattr(item, p) for p in props] for item in instances]
    if content:
        headers = [p.capitalize() for p in props]
        print(tabulate(content, headers))


@instance.command("drop")
@name_argument
@version_argument
@click.pass_obj
def instance_drop(ctx: Context, name: str, version: Optional[str]) -> None:
    """Drop a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.drop(ctx, instance)


@instance.command("status")
@name_argument
@version_argument
@click.pass_context
def instance_status(ctx: click.core.Context, name: str, version: Optional[str]) -> None:
    """Check the status of a PostgreSQL instance.

    Output the status string value ('running', 'not running', 'unspecified
    datadir') and exit with respective status code (0, 3, 4).
    """
    instance = get_instance(ctx.obj, name, version)
    with runner(ctx.obj):
        status = instance_mod.status(ctx.obj, instance)
    click.echo(status.name.replace("_", " "))
    ctx.exit(status.value)


@cli.command("start-instance")
@name_argument
@version_argument
@click.pass_obj
def start_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Start a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.start(ctx, instance)


@cli.command("stop-instance")
@name_argument
@version_argument
@click.pass_obj
def stop_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Stop a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.stop(ctx, instance)


@cli.command("reload-instance")
@name_argument
@version_argument
@click.pass_obj
def reload_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Reload a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.reload(ctx, instance)


@cli.command("restart-instance")
@name_argument
@version_argument
@click.pass_obj
def restart_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Restart a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.restart(ctx, instance)


@cli.command("backup-instance")
@name_argument
@version_argument
@click.option(
    "--type",
    type=click.Choice([t.name for t in pgbackrest.BackupType]),
    default=pgbackrest.BackupType.default().name,
    help="Backup type",
)
@click.option("--purge", is_flag=True, default=False, help="Purge old backups")
@click.pass_obj
def backup_instance(
    ctx: Context, name: str, version: Optional[str], type: str, purge: bool
) -> None:
    """Back up a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    pgbackrest.backup(ctx, instance, type=pgbackrest.BackupType(type))
    if purge:
        pgbackrest.expire(ctx, instance)


instance_identifier = click.argument("instance", metavar="<version>/<instance>")


@cli.group("role")
def role() -> None:
    """Manipulate roles"""


@role.command("schema")
def role_schema() -> None:
    """Print the JSON schema of role model"""
    print(manifest.Role.schema_json(indent=2))


@role.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("rb"), metavar="MANIFEST", required=True)
@click.pass_obj
def role_apply(ctx: Context, instance: str, file: IO[str]) -> None:
    """Apply manifest as a role"""
    i = instance_lookup(ctx, instance)
    with runner(ctx):
        roles.apply(ctx, i, manifest.Role.parse_yaml(file))


@role.command("describe")
@instance_identifier
@click.argument("name")
@click.pass_obj
def role_describe(ctx: Context, instance: str, name: str) -> None:
    """Describe a role"""
    i = instance_lookup(ctx, instance)
    try:
        described = roles.describe(ctx, i, name)
    except exceptions.RoleNotFound as e:
        raise click.ClickException(e.show())
    print(described.yaml(), end="")


@role.command("drop")
@instance_identifier
@click.argument("name")
@click.pass_obj
def role_drop(ctx: Context, instance: str, name: str) -> None:
    """Drop a role"""
    i = instance_lookup(ctx, instance)
    try:
        roles.drop(ctx, i, name)
    except exceptions.RoleNotFound as e:
        raise click.ClickException(e.show())
