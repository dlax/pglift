import functools
from typing import IO, Any, Sequence

import click
from pydantic.utils import deep_update
from rich.console import Console

from .. import instances, privileges, roles, task
from ..ctx import Context
from ..models import helpers, interface, system
from .util import (
    Group,
    as_json_option,
    instance_identifier_option,
    pass_console,
    pass_ctx,
    pass_instance,
    print_json_for,
    print_schema,
    print_table_for,
)


@click.group("role", cls=Group)
@instance_identifier_option
@click.option(
    "--schema",
    is_flag=True,
    callback=functools.partial(print_schema, model=interface.Role),
    expose_value=False,
    is_eager=True,
    help="Print the JSON schema of role model and exit.",
)
def cli(instance: system.Instance) -> None:
    """Manage roles."""


@cli.command("create")
@helpers.parameters_from_model(interface.Role, "create")
@pass_instance
@pass_ctx
def role_create(ctx: Context, instance: system.Instance, role: interface.Role) -> None:
    """Create a role in a PostgreSQL instance"""
    with instances.running(ctx, instance):
        if roles.exists(ctx, instance, role.name):
            raise click.ClickException("role already exists")
        with task.transaction():
            roles.apply(ctx, instance, role)


@cli.command("alter")
@helpers.parameters_from_model(interface.Role, "update", parse_model=False)
@click.argument("rolname")
@pass_instance
@pass_ctx
def role_alter(
    ctx: Context, instance: system.Instance, rolname: str, **changes: Any
) -> None:
    """Alter a role in a PostgreSQL instance"""
    changes = helpers.unnest(interface.Role, changes)
    with instances.running(ctx, instance):
        values = roles.get(ctx, instance, rolname).dict()
        values = deep_update(values, changes)
        altered = interface.Role.parse_obj(values)
        roles.apply(ctx, instance, altered)


@cli.command("apply", hidden=True)
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_instance
@pass_ctx
def role_apply(ctx: Context, instance: system.Instance, file: IO[str]) -> None:
    """Apply manifest as a role"""
    role = interface.Role.parse_yaml(file)
    with instances.running(ctx, instance):
        roles.apply(ctx, instance, role)


@cli.command("get")
@as_json_option
@click.argument("name")
@pass_instance
@pass_console
@pass_ctx
def role_get(
    ctx: Context, console: Console, instance: system.Instance, name: str, as_json: bool
) -> None:
    """Get the description of a role"""
    with instances.running(ctx, instance):
        m = roles.get(ctx, instance, name).dict(by_alias=True)
        del m["state"]
    if as_json:
        print_json_for(m, display=console.print_json)
    else:
        print_table_for([m], display=console.print, box=None)


@cli.command("drop")
@click.argument("name")
@pass_instance
@pass_ctx
def role_drop(ctx: Context, instance: system.Instance, name: str) -> None:
    """Drop a role"""
    with instances.running(ctx, instance):
        roles.drop(ctx, instance, name)


@cli.command("privileges")
@click.argument("name")
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@click.option("--default", "defaults", is_flag=True, help="Display default privileges")
@as_json_option
@pass_instance
@pass_console
@pass_ctx
def role_privileges(
    ctx: Context,
    console: Console,
    instance: system.Instance,
    name: str,
    databases: Sequence[str],
    defaults: bool,
    as_json: bool,
) -> None:
    """List privileges of a role."""
    with instances.running(ctx, instance):
        roles.get(ctx, instance, name)  # check existence
        try:
            prvlgs = privileges.get(
                ctx, instance, databases=databases, roles=(name,), defaults=defaults
            )
        except ValueError as e:
            raise click.ClickException(str(e))
    if as_json:
        print_json_for(
            (i.dict(by_alias=True) for i in prvlgs), display=console.print_json
        )
    else:
        print_table_for((i.dict(by_alias=True) for i in prvlgs), display=console.print)
