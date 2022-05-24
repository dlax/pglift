from typing import Optional

import attr
from pydantic import Field, SecretStr

from .. import types
from .._compat import Final

default_port: Final = 2345


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Service:
    """A temboard-agent service bound to a PostgreSQL instance."""

    port: int
    """TCP port for the temboard-agent API."""

    password: Optional[SecretStr]


class ServiceManifest(types.ServiceManifest, service_name="temboard"):
    port: types.Port = Field(
        default=types.Port(default_port),
        description="TCP port for the temboard-agent API.",
    )
    password: Optional[SecretStr] = Field(
        default=None,
        description="Password of PostgreSQL role for temboard agent.",
        writeOnly=True,
    )
