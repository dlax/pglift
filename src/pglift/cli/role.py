from typing import IO, Any, Sequence

import click
from pydantic.utils import deep_update
from rich.console import Console

from .. import instance as instance_mod
from .. import privileges, roles, task
from ..ctx import Context
from ..models import helpers, interface, system
from .util import (
    Group,
    as_json_option,
    instance_identifier,
    pass_console,
    pass_ctx,
    print_json_for,
    print_table_for,
)


@click.group("role", cls=Group)
def cli() -> None:
    """Manage roles."""


@cli.command("create")
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


@cli.command("alter")
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


@cli.command("schema")
@pass_console
def role_schema(console: Console) -> None:
    """Print the JSON schema of role model"""
    console.print_json(interface.Role.schema_json(indent=2))


@cli.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_ctx
def role_apply(ctx: Context, instance: system.Instance, file: IO[str]) -> None:
    """Apply manifest as a role"""
    role = interface.Role.parse_yaml(file)
    with instance_mod.running(ctx, instance):
        roles.apply(ctx, instance, role)


@cli.command("describe")
@instance_identifier
@click.argument("name")
@pass_ctx
def role_describe(ctx: Context, instance: system.Instance, name: str) -> None:
    """Describe a role"""
    with instance_mod.running(ctx, instance):
        described = roles.describe(ctx, instance, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@cli.command("drop")
@instance_identifier
@click.argument("name")
@pass_ctx
def role_drop(ctx: Context, instance: system.Instance, name: str) -> None:
    """Drop a role"""
    with instance_mod.running(ctx, instance):
        roles.drop(ctx, instance, name)


@cli.command("privileges")
@instance_identifier
@click.argument("name")
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@as_json_option
@pass_console
@pass_ctx
def role_privileges(
    ctx: Context,
    console: Console,
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
        print_json_for(prvlgs, display=console.print_json)
    else:
        print_table_for(prvlgs, display=console.print)
