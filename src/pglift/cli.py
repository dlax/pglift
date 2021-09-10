import json
import logging
import os
import tempfile
from datetime import datetime
from functools import partial
from typing import IO, Any, Callable, Iterable, Optional, Sequence, TypeVar, Union

import click
import pydantic.json
from pydantic.utils import deep_update
from tabulate import tabulate
from typing_extensions import Literal

from . import __name__ as pkgname
from . import _install, databases, exceptions
from . import instance as instance_mod
from . import pgbackrest, pm, privileges, prometheus, roles
from .ctx import Context
from .instance import Status
from .models import helpers, interface
from .models.system import Instance
from .settings import POSTGRESQL_SUPPORTED_VERSIONS
from .task import runner

log_formatter = logging.Formatter(
    fmt="%(levelname)s:%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Command(click.Command):
    def invoke(self, ctx: click.Context) -> Any:
        logfile = tempfile.NamedTemporaryFile(
            prefix=f"{pkgname}-", suffix=".log", delete=False
        ).name
        logger = logging.getLogger(pkgname)
        handler = logging.FileHandler(logfile)
        handler.setFormatter(log_formatter)
        logger.addHandler(handler)
        try:
            return super().invoke(ctx)
        except exceptions.Error as e:
            msg = str(e)
            if isinstance(e, exceptions.CommandError) and e.stderr:
                msg += f"\n{e.stderr}"
            raise click.ClickException(msg)
        except (click.ClickException, click.Abort, click.exceptions.Exit):
            raise
        except pydantic.ValidationError as e:
            raise click.ClickException(str(e))
        except Exception:
            logger.exception("an unexpected error occurred")
            raise click.ClickException(
                "an unexpected error occurred, this is probably a bug; "
                f"details can be found at {logfile}"
            )
        os.unlink(logfile)


class Group(click.Group):
    command_class = Command
    group_class = type


def get_instance(ctx: Context, name: str, version: Optional[str]) -> Instance:
    try:
        return Instance.system_lookup(ctx, (name, version))
    except Exception as e:
        raise click.ClickException(str(e))


def instance_lookup(ctx: Context, instance_id: str) -> Instance:
    version = None
    try:
        version, name = instance_id.split("/", 1)
    except ValueError:
        name = instance_id
    return get_instance(ctx, name, version)


_M = TypeVar("_M", bound=pydantic.BaseModel)


def print_table_for(
    items: Iterable[_M], display: Callable[[str], None] = click.echo
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
    >>> print_table_for(items, display=print)
    name    address        address  address
            street             zip  city
    ------  -----------  ---------  ---------
    bob     main street      31234  luz
    """
    values = []
    for item in items:
        d = item.dict(by_alias=True)
        for k, v in list(d.items()):
            if isinstance(v, dict):
                for sk, sv in v.items():
                    mk = f"{k}\n{sk}"
                    assert mk not in d, mk
                    d[mk] = sv
                del d[k]
        values.append(d)
    display(tabulate(values, headers="keys"))


def print_json_for(
    items: Iterable[_M], display: Callable[[str], None] = partial(click.echo, nl=False)
) -> None:
    """Render a list of items as JSON.

    >>> class Foo(pydantic.BaseModel):
    ...     bar_: str = pydantic.Field(alias="bar")
    ...     baz: int
    >>> items = [Foo(bar="x", baz=1), Foo(bar="y", baz=3)]
    >>> print_json_for(items, display=print)
    [{"bar": "x", "baz": 1}, {"bar": "y", "baz": 3}]
    """
    display(
        json.dumps(
            [i.dict(by_alias=True) for i in items],
            default=pydantic.json.pydantic_encoder,
        ),
    )


as_json_option = click.option("--json", "as_json", is_flag=True, help="Print as JSON")


@click.group(cls=Group)
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
)
@click.pass_context
def cli(ctx: click.core.Context, log_level: Optional[str]) -> None:
    """Deploy production-ready instances of PostgreSQL"""
    logger = logging.getLogger(pkgname)
    if log_level is not None:
        handler = logging.StreamHandler()
        handler.setFormatter(log_formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)

    if not ctx.obj:
        ctx.obj = Context(plugin_manager=pm.PluginManager.get())


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
        env = f"SETTINGS=@{settings}" if settings else None
        _install.do(ctx, env=env)
    elif action == "uninstall":
        _install.undo(ctx)


@cli.group("instance")
def instance() -> None:
    """Manipulate instances"""


@instance.command("init")
@helpers.parameters_from_model(interface.Instance)
@click.pass_obj
def instance_init(ctx: Context, instance: interface.Instance) -> None:
    """Initialize a PostgreSQL instance"""
    if instance.spec(ctx).exists():
        raise click.ClickException("instance already exists")
    with runner(ctx):
        instance_mod.apply(ctx, instance)


@instance.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@click.pass_obj
def instance_apply(ctx: Context, file: IO[str]) -> None:
    """Apply manifest as a PostgreSQL instance"""
    instance = interface.Instance.parse_yaml(file)
    with runner(ctx):
        instance_mod.apply(ctx, instance)


@instance.command("alter")
@helpers.parameters_from_model(interface.Instance, False)
@click.pass_obj
def instance_alter(
    ctx: Context, name: str, version: Optional[str] = None, **changes: Any
) -> None:
    """Alter a PostgreSQL instance"""
    changes = helpers.unnest(interface.Instance, changes)
    values = instance_mod.describe(ctx, name, version).dict()
    values = deep_update(values, changes)
    altered = interface.Instance.parse_obj(values)
    with runner(ctx):
        instance_mod.apply(ctx, altered)


@instance.command("schema")
def instance_schema() -> None:
    """Print the JSON schema of PostgreSQL instance model"""
    click.echo(interface.Instance.schema_json(indent=2), nl=False)


name_argument = click.argument("name", type=click.STRING)
version_argument = click.argument("version", required=False, type=click.STRING)


@instance.command("describe")
@name_argument
@version_argument
@click.pass_obj
def instance_describe(ctx: Context, name: str, version: Optional[str]) -> None:
    """Describe a PostgreSQL instance"""
    described = instance_mod.describe(ctx, name, version)
    click.echo(described.yaml(), nl=False)


@instance.command("list")
@click.option(
    "--version",
    type=click.Choice(POSTGRESQL_SUPPORTED_VERSIONS),
    help="Only list instances of specified version.",
)
@as_json_option
@click.pass_obj
def instance_list(ctx: Context, version: Optional[str], as_json: bool) -> None:
    """List the available instances"""

    instances = instance_mod.list(ctx, version=version)
    if as_json:
        print_json_for(instances)
    else:
        print_table_for(instances)


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


@instance.command("start")
@name_argument
@version_argument
@click.pass_obj
def instance_start(ctx: Context, name: str, version: Optional[str]) -> None:
    """Start a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    instance_mod.check_status(ctx, instance, Status.not_running)
    with runner(ctx):
        instance_mod.start(ctx, instance)


@instance.command("stop")
@name_argument
@version_argument
@click.pass_obj
def instance_stop(ctx: Context, name: str, version: Optional[str]) -> None:
    """Stop a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.stop(ctx, instance)


@instance.command("reload")
@name_argument
@version_argument
@click.pass_obj
def instance_reload(ctx: Context, name: str, version: Optional[str]) -> None:
    """Reload a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.reload(ctx, instance)


@instance.command("restart")
@name_argument
@version_argument
@click.pass_obj
def instance_restart(ctx: Context, name: str, version: Optional[str]) -> None:
    """Restart a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    with runner(ctx):
        instance_mod.restart(ctx, instance)


@instance.command("shell")
@name_argument
@version_argument
@click.option(
    "-d",
    "--dbname",
    metavar="DBNAME",
    envvar="PGDATABASE",
    help="database name to connect to",
)
@click.option(
    "-U",
    "--user",
    metavar="USER",
    envvar="PGUSER",
    help="database user name",
)
@click.pass_obj
def instance_shell(
    ctx: Context, name: str, version: Optional[str], user: str, dbname: Optional[str]
) -> None:
    """Open a PostgreSQL interactive shell on a running instance."""
    instance = get_instance(ctx, name, version)
    instance_mod.check_status(ctx, instance, Status.running)
    instance_mod.shell(ctx, instance, user=user, dbname=dbname)


@instance.command("backup")
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
def instance_backup(
    ctx: Context, name: str, version: Optional[str], type: str, purge: bool
) -> None:
    """Back up a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    pgbackrest.backup(ctx, instance, type=pgbackrest.BackupType(type))
    if purge:
        pgbackrest.expire(ctx, instance)


@instance.command("restore")
@name_argument
@version_argument
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
@click.pass_obj
def instance_restore(
    ctx: Context,
    name: str,
    version: Optional[str],
    list_only: bool,
    label: Optional[str],
    date: Optional[datetime],
) -> None:
    """Restore a PostgreSQL instance"""
    instance = get_instance(ctx, name, version)
    if list_only:
        backups = pgbackrest.iter_backups(ctx, instance)
        print_table_for(backups)
    else:
        instance_mod.check_status(ctx, instance, Status.not_running)
        if label is not None and date is not None:
            raise click.BadArgumentUsage(
                "--label and --date arguments are mutually exclusive"
            )
        pgbackrest.restore(ctx, instance, label=label, date=date)


@instance.command("privileges")
@name_argument
@version_argument
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@as_json_option
@click.pass_obj
def instance_privileges(
    ctx: Context,
    name: str,
    version: Optional[str],
    databases: Sequence[str],
    roles: Sequence[str],
    as_json: bool,
) -> None:
    """List default privileges on instance."""
    instance = get_instance(ctx, name, version)
    with instance_mod.running(ctx, instance):
        try:
            prvlgs = privileges.get(ctx, instance, databases=databases, roles=roles)
        except ValueError as e:
            raise click.ClickException(str(e))
    if as_json:
        print_json_for(prvlgs)
    else:
        print_table_for(prvlgs)


instance_identifier = click.argument("instance", metavar="<version>/<instance>")


@cli.group("role")
def role() -> None:
    """Manipulate roles"""


@role.command("create")
@instance_identifier
@helpers.parameters_from_model(interface.Role)
@click.pass_obj
def role_create(ctx: Context, instance: str, role: interface.Role) -> None:
    """Create a role in a PostgreSQL instance"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        if roles.exists(ctx, i, role.name):
            raise click.ClickException("role already exists")
        with runner(ctx):
            roles.apply(ctx, i, role)


@role.command("alter")
@instance_identifier
@helpers.parameters_from_model(interface.Role, False)
@click.pass_obj
def role_alter(ctx: Context, instance: str, name: str, **changes: Any) -> None:
    """Alter a role in a PostgreSQL instance"""
    i = instance_lookup(ctx, instance)
    changes = helpers.unnest(interface.Role, changes)
    with instance_mod.running(ctx, i):
        values = roles.describe(ctx, i, name).dict()
        values = deep_update(values, changes)
        altered = interface.Role.parse_obj(values)
        with runner(ctx):
            roles.apply(ctx, i, altered)


@role.command("schema")
def role_schema() -> None:
    """Print the JSON schema of role model"""
    click.echo(interface.Role.schema_json(indent=2), nl=False)


@role.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@click.pass_obj
def role_apply(ctx: Context, instance: str, file: IO[str]) -> None:
    """Apply manifest as a role"""
    i = instance_lookup(ctx, instance)
    role = interface.Role.parse_yaml(file)
    with runner(ctx), instance_mod.running(ctx, i):
        roles.apply(ctx, i, role)


@role.command("describe")
@instance_identifier
@click.argument("name")
@click.pass_obj
def role_describe(ctx: Context, instance: str, name: str) -> None:
    """Describe a role"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        described = roles.describe(ctx, i, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@role.command("drop")
@instance_identifier
@click.argument("name")
@click.pass_obj
def role_drop(ctx: Context, instance: str, name: str) -> None:
    """Drop a role"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        roles.drop(ctx, i, name)


@role.command("privileges")
@instance_identifier
@click.argument("name")
@click.option(
    "-d", "--database", "databases", multiple=True, help="Database to inspect"
)
@as_json_option
@click.pass_obj
def role_privileges(
    ctx: Context,
    instance: str,
    name: str,
    databases: Sequence[str],
    as_json: bool,
) -> None:
    """List default privileges of a role."""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        roles.describe(ctx, i, name)  # check existence
        try:
            prvlgs = privileges.get(ctx, i, databases=databases, roles=(name,))
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
@click.pass_obj
def database_create(ctx: Context, instance: str, database: interface.Database) -> None:
    """Create a database in a PostgreSQL instance"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        if databases.exists(ctx, i, database.name):
            raise click.ClickException("database already exists")
        with runner(ctx):
            databases.apply(ctx, i, database)


@database.command("alter")
@instance_identifier
@helpers.parameters_from_model(interface.Database, False)
@click.pass_obj
def database_alter(ctx: Context, instance: str, name: str, **changes: Any) -> None:
    """Alter a database in a PostgreSQL instance"""
    i = instance_lookup(ctx, instance)
    changes = helpers.unnest(interface.Database, changes)
    with instance_mod.running(ctx, i):
        values = databases.describe(ctx, i, name).dict()
        values = deep_update(values, changes)
        altered = interface.Database.parse_obj(values)
        with runner(ctx):
            databases.apply(ctx, i, altered)


@database.command("schema")
def database_schema() -> None:
    """Print the JSON schema of database model"""
    click.echo(interface.Database.schema_json(indent=2), nl=False)


@database.command("apply")
@instance_identifier
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@click.pass_obj
def database_apply(ctx: Context, instance: str, file: IO[str]) -> None:
    """Apply manifest as a database"""
    i = instance_lookup(ctx, instance)
    database = interface.Database.parse_yaml(file)
    with runner(ctx), instance_mod.running(ctx, i):
        databases.apply(ctx, i, database)


@database.command("describe")
@instance_identifier
@click.argument("name")
@click.pass_obj
def database_describe(ctx: Context, instance: str, name: str) -> None:
    """Describe a database"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        described = databases.describe(ctx, i, name)
    click.echo(described.yaml(exclude={"state"}), nl=False)


@database.command("list")
@instance_identifier
@as_json_option
@click.pass_obj
def database_list(ctx: Context, instance: str, as_json: bool) -> None:
    """List databases"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        dbs = databases.list(ctx, i)
    if as_json:
        print_json_for(dbs)
    else:
        print_table_for(dbs)


@database.command("drop")
@instance_identifier
@click.argument("name")
@click.pass_obj
def database_drop(ctx: Context, instance: str, name: str) -> None:
    """Drop a database"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        databases.drop(ctx, i, name)


@database.command("privileges")
@instance_identifier
@click.argument("name")
@click.option("-r", "--role", "roles", multiple=True, help="Role to inspect")
@as_json_option
@click.pass_obj
def database_privileges(
    ctx: Context,
    instance: str,
    name: str,
    roles: Sequence[str],
    as_json: bool,
) -> None:
    """List default privileges on a database."""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        databases.describe(ctx, i, name)  # check existence
        try:
            prvlgs = privileges.get(ctx, i, databases=(name,), roles=roles)
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
@click.pass_obj
def database_run(
    ctx: Context,
    instance: str,
    sql_command: str,
    dbnames: Sequence[str],
    exclude_dbnames: Sequence[str],
) -> None:
    """Run given command on databases of a PostgreSQL instance"""
    i = instance_lookup(ctx, instance)
    with instance_mod.running(ctx, i):
        databases.run(
            ctx, i, sql_command, dbnames=dbnames, exclude_dbnames=exclude_dbnames
        )


@cli.group("postgres_exporter")
def postgres_exporter() -> None:
    """Handle Prometheus postgres_exporter"""


@postgres_exporter.command("start")
@instance_identifier
@click.pass_obj
def postgres_exporter_start(ctx: Context, instance: str) -> None:
    """Start postgres_exporter."""
    i = instance_lookup(ctx, instance)
    prometheus.start(ctx, i)


@postgres_exporter.command("stop")
@instance_identifier
@click.pass_obj
def postgres_exporter_stop(ctx: Context, instance: str) -> None:
    """Stop postgres_exporter."""
    i = instance_lookup(ctx, instance)
    prometheus.stop(ctx, i)
