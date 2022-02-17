import json
import logging
import os
import pathlib
import tempfile
import time
from functools import wraps
from types import ModuleType
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar, cast

import click
import psycopg
import pydantic
import pydantic.json
import rich
from click.exceptions import Exit
from click.shell_completion import CompletionItem
from rich.console import Console, RenderableType
from rich.table import Table

from .. import __name__ as pkgname
from .. import exceptions
from .. import instance as instance_mod
from .. import task
from ..ctx import Context
from ..models import system
from ..settings import POSTGRESQL_SUPPORTED_VERSIONS, Settings

logger = logging.getLogger(pkgname)

_M = TypeVar("_M", bound=pydantic.BaseModel)


def print_table_for(
    items: Iterable[_M],
    title: Optional[str] = None,
    *,
    display: Callable[[RenderableType], None] = rich.print,
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
    >>> print_table_for(items, title="address book")  # doctest: +NORMALIZE_WHITESPACE
                   address book
    ┏━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┓
    ┃      ┃ address     ┃ address ┃ address ┃
    ┃ name ┃ street      ┃ zip     ┃ city    ┃
    ┡━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━┩
    │ bob  │ main street │ 31234   │ luz     │
    └──────┴─────────────┴─────────┴─────────┘
    """
    table = None
    headers: List[str] = []
    rows = []
    for item in items:
        d = item.dict(by_alias=True)
        row = []
        hdr = []
        for k, v in list(d.items()):
            if isinstance(v, dict):
                for sk, sv in v.items():
                    mk = f"{k}\n{sk}"
                    hdr.append(mk)
                    row.append(sv)
            else:
                hdr.append(k)
                row.append(v)
        if not headers:
            headers = hdr[:]
        rows.append([str(v) for v in row])
    if not rows:
        return
    table = Table(*headers, title=title)
    for row in rows:
        table.add_row(*row)
    display(table)


def print_json_for(
    items: Iterable[_M], *, display: Callable[[str], None] = rich.print_json
) -> None:
    """Render a list of items as JSON.

    >>> class Foo(pydantic.BaseModel):
    ...     bar_: str = pydantic.Field(alias="bar")
    ...     baz: int
    >>> items = [Foo(bar="x", baz=1), Foo(bar="y", baz=3)]
    >>> print_json_for(items, display=rich.print)
    [{"bar": "x", "baz": 1}, {"bar": "y", "baz": 3}]
    """
    display(
        json.dumps(
            [i.dict(by_alias=True) for i in items],
            default=pydantic.json.pydantic_encoder,
        ),
    )


C = TypeVar("C", bound=Callable[..., Any])


def pass_ctx(f: C) -> C:
    """Command decorator passing 'Context' bound to click.Context's object."""

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        context = click.get_current_context()
        ctx = context.obj.ctx
        assert isinstance(ctx, Context), ctx
        return context.invoke(f, ctx, *args, **kwargs)

    return cast(C, wrapper)


def pass_console(f: C) -> C:
    """Command decorator passing 'Console' bound to click.Context's object."""

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        context = click.get_current_context()
        console = context.obj.console
        assert isinstance(console, Console), console
        return context.invoke(f, console, *args, **kwargs)

    return cast(C, wrapper)


def pass_component_settings(mod: ModuleType, name: str, f: C) -> C:
    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        context = click.get_current_context()
        ctx = context.obj.ctx
        assert isinstance(ctx, Context), ctx
        settings = getattr(mod, "available")(ctx)
        if not settings:
            click.echo(f"{name} not available", err=True)
            raise Exit(1)
        context.invoke(f, settings, *args, **kwargs)

    return cast(C, wrapper)


def get_instance(ctx: Context, name: str, version: Optional[str]) -> system.Instance:
    """Return an Instance from name/version, possibly guessing version if unspecified."""
    if version is None:
        found = None
        for version in POSTGRESQL_SUPPORTED_VERSIONS:
            try:
                instance = system.Instance.system_lookup(ctx, (name, version))
            except exceptions.InstanceNotFound:
                logger.debug("instance '%s' not found in version %s", name, version)
            else:
                if found:
                    raise click.BadParameter(
                        f"instance '{name}' exists in several PostgreSQL versions;"
                        " please select version explicitly"
                    )
                found = instance

        if found:
            return found

        raise click.BadParameter(f"instance '{name}' not found")

    try:
        return system.Instance.system_lookup(ctx, (name, version))
    except Exception as e:
        raise click.BadParameter(str(e))


def nameversion_from_id(instance_id: str) -> Tuple[str, Optional[str]]:
    version = None
    try:
        version, name = instance_id.split("/", 1)
    except ValueError:
        name = instance_id
    return name, version


def instance_lookup(
    context: click.Context, param: click.Parameter, value: str
) -> system.Instance:
    name, version = nameversion_from_id(value)
    ctx = context.obj.ctx
    return get_instance(ctx, name, version)


def _list_instances(
    context: click.Context, param: click.Parameter, incomplete: str
) -> List[CompletionItem]:
    """Shell completion function for instance identifier <name> or <version>/<name>."""
    out = []
    iname, iversion = nameversion_from_id(incomplete)
    ctx = Context(settings=Settings())
    for i in instance_mod.list(ctx):
        if iversion is not None and i.version.startswith(iversion):
            if i.name.startswith(iname):
                out.append(
                    CompletionItem(f"{i.version}/{i.name}", help=f"port={i.port}")
                )
            else:
                out.append(CompletionItem(i.version))
        else:
            out.append(
                CompletionItem(i.name, help=f"{i.version}/{i.name} port={i.port}")
            )
    return out


instance_identifier = click.argument(
    "instance",
    metavar="<version>/<name>",
    callback=instance_lookup,
    shell_complete=_list_instances,
)


as_json_option = click.option("--json", "as_json", is_flag=True, help="Print as JSON")


def validate_foreground(
    context: click.Context, param: click.Parameter, value: bool
) -> bool:
    ctx = context.obj.ctx
    if ctx.settings.service_manager == "systemd" and value:
        raise click.BadParameter("cannot be used with systemd")
    return value


foreground_option = click.option(
    "--foreground",
    is_flag=True,
    help="Start the program in foreground.",
    callback=validate_foreground,
)


class Command(click.Command):
    def invoke(self, context: click.Context) -> Any:
        ctx = context.obj.ctx
        displayer = context.obj.displayer
        logger = logging.getLogger(pkgname)
        logdir = ctx.settings.logpath
        logdir.mkdir(parents=True, exist_ok=True)
        logfilename = f"{time.time()}.log"
        logfile = logdir / logfilename
        try:
            handler = logging.FileHandler(logfile)
        except OSError:
            # Might be, e.g. PermissionError, if log file path is not writable.
            logfile = pathlib.Path(
                tempfile.NamedTemporaryFile(prefix="pglift", suffix=logfilename).name
            )
            handler = logging.FileHandler(logfile)
        formatter = logging.Formatter(
            fmt="%(levelname)-8s - %(asctime)s - %(name)s:%(filename)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        keep_logfile = False
        try:
            try:
                with task.displayer_installed(displayer):
                    return super().invoke(context)
            except exceptions.Cancelled as e:
                logger.warning(str(e))
                raise click.Abort
            except exceptions.Error as e:
                logger.debug("an internal error occurred", exc_info=True)
                msg = str(e)
                if isinstance(e, exceptions.CommandError):
                    if e.stderr:
                        msg += f"\n{e.stderr}"
                    if e.stdout:
                        msg += f"\n{e.stdout}"
                raise click.ClickException(msg)
            except (click.ClickException, click.Abort, click.exceptions.Exit):
                raise
            except pydantic.ValidationError as e:
                logger.debug("a validation error occurred", exc_info=True)
                raise click.ClickException(str(e))
            except psycopg.OperationalError as e:
                logger.debug("an operational error occurred", exc_info=True)
                raise click.ClickException(str(e).strip())
            except Exception:
                keep_logfile = True
                logger.exception("an unexpected error occurred")
                raise click.ClickException(
                    "an unexpected error occurred, this is probably a bug; "
                    f"details can be found at {logfile}"
                )
        finally:
            if not keep_logfile:
                os.unlink(logfile)
                if next(logfile.parent.iterdir(), None) is None:
                    logfile.parent.rmdir()


class Group(click.Group):
    command_class = Command
    group_class = type
