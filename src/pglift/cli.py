import json
from typing import IO, Optional

import click
import pydantic.json
from tabulate import tabulate

from . import instance as instance_mod
from . import manifest, pgbackrest, pm
from .ctx import Context
from .model import Instance, InstanceSpec
from .settings import SETTINGS
from .task import runner


@click.group()
@click.pass_context
def cli(ctx: click.core.Context) -> None:
    """Deploy production-ready instances of PostgreSQL"""

    if not ctx.obj:
        ctx.obj = Context(plugin_manager=pm.PluginManager.get(), settings=SETTINGS)


@cli.group("instance")
def instance() -> None:
    """Manipulate instances"""


@instance.command("apply")
@click.option("-f", "--file", type=click.File("rb"), metavar="MANIFEST", required=True)
@click.pass_obj
def instance_apply(ctx: Context, file: IO[str]) -> None:
    """Apply manifest as a PostgreSQL instance"""
    with runner():
        instance_mod.apply(ctx, manifest.Instance.parse_yaml(file))


@instance.command("schema")
def instance_schema() -> None:
    """Print the JSON schema of PostgreSQL instance model"""
    print(manifest.Instance.schema_json(indent=2))


name_argument = click.argument("name", type=click.STRING)
version_argument = click.argument("version", required=False, type=click.STRING)


def get_instance(ctx: Context, name: str, version: Optional[str]) -> InstanceSpec:
    if version:
        return InstanceSpec(name, version, settings=ctx.settings)
    else:
        return InstanceSpec.default_version(name, ctx)


@instance.command("describe")
@name_argument
@version_argument
@click.pass_obj
def instance_describe(ctx: Context, name: str, version: Optional[str]) -> None:
    """Describe a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    described = instance_mod.describe(ctx, instance)
    if described:
        print(described.yaml(), end="")


@instance.command("list")
@click.option("--json", "as_json", is_flag=True, help="Print as JSON")
@click.pass_obj
def instance_list(ctx: Context, as_json: bool) -> None:
    """List the available instances"""

    if as_json:
        print(
            json.dumps(
                list(instance_mod.list(ctx)), default=pydantic.json.pydantic_encoder
            )
        )
        return

    props = manifest.InstanceListItem.__fields__
    headers = [p.capitalize() for p in props]
    content = [[getattr(info, p) for p in props] for info in instance_mod.list(ctx)]
    print(tabulate(content, headers))


@instance.command("drop")
@name_argument
@version_argument
@click.pass_obj
def instance_drop(ctx: Context, name: str, version: Optional[str]) -> None:
    """Drop a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner():
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
    with runner():
        status = instance_mod.status(ctx.obj, instance)
    click.echo(status.name.replace("_", " "))
    ctx.exit(status.value)


@cli.command("start-instance")
@name_argument
@version_argument
@click.pass_obj
def start_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Start a PostgreSQL instance"""
    instance = Instance.from_spec(get_instance(ctx, name, version))
    with runner():
        instance_mod.start(ctx, instance)


@cli.command("stop-instance")
@name_argument
@version_argument
@click.pass_obj
def stop_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Stop a PostgreSQL instance"""
    instance = Instance.from_spec(get_instance(ctx, name, version))
    with runner():
        instance_mod.stop(ctx, instance)


@cli.command("reload-instance")
@name_argument
@version_argument
@click.pass_obj
def reload_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Reload a PostgreSQL instance"""
    instance = Instance.from_spec(get_instance(ctx, name, version))
    with runner():
        instance_mod.reload(ctx, instance)


@cli.command("restart-instance")
@name_argument
@version_argument
@click.pass_obj
def restart_instance(ctx: Context, name: str, version: Optional[str]) -> None:
    """Restart a PostgreSQL instance"""
    instance = Instance.from_spec(get_instance(ctx, name, version))
    with runner():
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
    instance = Instance.from_spec(get_instance(ctx, name, version))
    pgbackrest.backup(ctx, instance, type=pgbackrest.BackupType(type))
    if purge:
        pgbackrest.expire(ctx, instance)
