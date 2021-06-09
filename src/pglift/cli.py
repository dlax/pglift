from typing import IO, Optional

import click

from . import instance as instance_mod
from . import manifest, pm
from .ctx import Context
from .model import Instance
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


def get_instance(ctx: Context, name: str, version: Optional[str]) -> Instance:
    if version:
        return Instance(name, version)
    else:
        return Instance.default_version(name, ctx)


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


@instance.command("drop")
@name_argument
@version_argument
@click.pass_obj
def instance_drop(ctx: Context, name: str, version: Optional[str]) -> None:
    """Drop a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner():
        instance_mod.drop(ctx, instance)
