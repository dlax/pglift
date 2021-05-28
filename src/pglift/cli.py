import click

from . import pm
from .ctx import Context
from .settings import SETTINGS


@click.group()
@click.pass_context
def cli(ctx: click.core.Context) -> None:
    """Deploy production-ready instances of PostgreSQL"""

    if not ctx.obj:
        ctx.obj = Context(plugin_manager=pm.PluginManager.get(), settings=SETTINGS)
