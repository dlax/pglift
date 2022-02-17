from typing import IO, Any, Sequence

import click
from pydantic.utils import deep_update
from rich.console import Console

from .. import databases
from .. import instance as instance_mod
from .. import privileges, task
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


@click.group("database", cls=Group)
def cli() -> None:
    """Manage databases."""


@cli.command("create")
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


@cli.command("alter")
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


@cli.command("schema")
@pass_console
def database_schema(console: Console) -> None:
    """Print the JSON schema of database model"""
    console.print_json(interface.Database.schema_json(indent=2))


@cli.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_ctx
def database_apply(ctx: Context, instance: system.Instance, file: IO[str]) -> None:
    """Apply manifest as a database"""
    database = interface.Database.parse_yaml(file)
    with instance_mod.running(ctx, instance):
        databases.apply(ctx, instance, database)


@cli.command("describe")
@instance_identifier
@click.argument("name")
@pass_ctx
def database_describe(ctx: Context, instance: system.Instance, name: str) -> None:
    """Describe a database"""
    with instance_mod.running(ctx, instance):
        described = databases.describe(ctx, instance, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@cli.command("list")
@instance_identifier
@as_json_option
@pass_console
@pass_ctx
def database_list(
    ctx: Context, console: Console, instance: system.Instance, as_json: bool
) -> None:
    """List databases"""
    with instance_mod.running(ctx, instance):
        dbs = databases.list(ctx, instance)
    if as_json:
        print_json_for(dbs, display=console.print_json)
    else:
        print_table_for(dbs, display=console.print)


@cli.command("drop")
@instance_identifier
@click.argument("name")
@pass_ctx
def database_drop(ctx: Context, instance: system.Instance, name: str) -> None:
    """Drop a database"""
    with instance_mod.running(ctx, instance):
        databases.drop(ctx, instance, name)


@cli.command("privileges")
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


@cli.command("run")
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
