import functools
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
    instance_identifier_option,
    pass_console,
    pass_ctx,
    pass_instance,
    print_json_for,
    print_schema,
    print_table_for,
)


@click.group("database", cls=Group)
@instance_identifier_option
@click.option(
    "--schema",
    is_flag=True,
    callback=functools.partial(print_schema, model=interface.Database),
    expose_value=False,
    is_eager=True,
    help="Print the JSON schema of database model and exit.",
)
def cli(instance: system.Instance) -> None:
    """Manage databases."""


@cli.command("create")
@helpers.parameters_from_model(interface.Database, "create")
@pass_instance
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
@helpers.parameters_from_model(interface.Database, "update", parse_model=False)
@click.argument("dbname")
@pass_instance
@pass_ctx
def database_alter(
    ctx: Context, instance: system.Instance, dbname: str, **changes: Any
) -> None:
    """Alter a database in a PostgreSQL instance"""
    changes = helpers.unnest(interface.Database, changes)
    with instance_mod.running(ctx, instance):
        values = databases.describe(ctx, instance, dbname).dict()
        values = deep_update(values, changes)
        altered = interface.Database.parse_obj(values)
        databases.apply(ctx, instance, altered)


@cli.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_instance
@pass_ctx
def database_apply(ctx: Context, instance: system.Instance, file: IO[str]) -> None:
    """Apply manifest as a database"""
    database = interface.Database.parse_yaml(file)
    with instance_mod.running(ctx, instance):
        databases.apply(ctx, instance, database)


@cli.command("describe")
@click.argument("name")
@pass_instance
@pass_ctx
def database_describe(ctx: Context, instance: system.Instance, name: str) -> None:
    """Describe a database"""
    with instance_mod.running(ctx, instance):
        described = databases.describe(ctx, instance, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@cli.command("list")
@as_json_option
@pass_instance
@pass_console
@pass_ctx
def database_list(
    ctx: Context, console: Console, instance: system.Instance, as_json: bool
) -> None:
    """List databases"""
    with instance_mod.running(ctx, instance):
        dbs = databases.list(ctx, instance)
    if as_json:
        print_json_for((i.dict(by_alias=True) for i in dbs), display=console.print_json)
    else:
        print_table_for((i.dict(by_alias=True) for i in dbs), display=console.print)


@cli.command("drop")
@click.argument("name")
@pass_instance
@pass_ctx
def database_drop(ctx: Context, instance: system.Instance, name: str) -> None:
    """Drop a database"""
    with instance_mod.running(ctx, instance):
        databases.drop(ctx, instance, name)


@cli.command("privileges")
@click.argument("name")
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@as_json_option
@pass_instance
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
        print_json_for((i.dict(by_alias=True) for i in prvlgs))
    else:
        print_table_for((i.dict(by_alias=True) for i in prvlgs))


@cli.command("run")
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
@as_json_option
@pass_instance
@pass_ctx
def database_run(
    ctx: Context,
    instance: system.Instance,
    sql_command: str,
    dbnames: Sequence[str],
    exclude_dbnames: Sequence[str],
    as_json: bool,
) -> None:
    """Run given command on databases of a PostgreSQL instance"""
    with instance_mod.running(ctx, instance):
        result = databases.run(
            ctx, instance, sql_command, dbnames=dbnames, exclude_dbnames=exclude_dbnames
        )
    if as_json:
        print_json_for(result)
    else:
        for dbname, rows in result.items():
            print_table_for(rows, title=f"Database {dbname}")
