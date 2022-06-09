from functools import partial
from typing import TYPE_CHECKING

import click

from .. import temboard
from ..cli.util import (
    Group,
    instance_identifier_option,
    pass_component_settings,
    pass_instance,
)
from . import impl

if TYPE_CHECKING:
    from ..models import system
    from ..settings import TemboardSettings

pass_temboard_settings = partial(pass_component_settings, temboard, "Temboard Agent")


@click.group("temboard-agent", cls=Group)
@instance_identifier_option
def temboard_agent(instance: "system.Instance") -> None:
    """Handle temBoard agent"""


@temboard_agent.command("secret-key")
@pass_temboard_settings
@pass_instance
def temboard_agent_secret_key(
    instance: "system.Instance", settings: "TemboardSettings"
) -> None:
    click.echo(impl.secret_key(instance.qualname, settings))
