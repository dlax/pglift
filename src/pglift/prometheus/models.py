import enum
from typing import ClassVar, Dict, Optional

import attr
import psycopg
import psycopg.conninfo
from pydantic import Field, SecretStr, validator
from typing_extensions import Final

from .. import types

default_port: Final = 9187


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Service:
    """A Prometheus postgres_exporter service bound to a PostgreSQL instance."""

    port: int
    """TCP port for the web interface and telemetry."""


class ServiceManifest(types.ServiceManifest, service_name="prometheus"):
    port: int = Field(
        default=default_port,
        description="TCP port for the web interface and telemetry of Prometheus",
    )


class PostgresExporter(types.Manifest):
    """Prometheus postgres_exporter service."""

    class State(types.AutoStrEnum):
        """Runtime state"""

        started = enum.auto()
        stopped = enum.auto()
        absent = enum.auto()

    _cli_config: ClassVar[Dict[str, types.CLIConfig]] = {
        "state": {"choices": [State.started.value, State.stopped.value]},
    }

    name: str = Field(description="locally unique identifier of the service")
    dsn: str = Field(description="connection string of target instance")
    password: Optional[SecretStr] = Field(description="connection password")
    port: int = Field(description="TCP port for the web interface and telemetry")
    state: State = Field(default=State.started, description="runtime state")

    @validator("name")
    def __validate_name_(cls, v: str) -> str:
        """Validate 'name' field.

        >>> PostgresExporter(name='without-slash', dsn="", port=12)  # doctest: +ELLIPSIS
        PostgresExporter(name='without-slash', ...)
        >>> PostgresExporter(name='with/slash', dsn="", port=12)
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for PostgresExporter
        name
          must not contain slashes (type=value_error)
        """
        # Avoid slash as this will break file paths during settings templating
        # (configpath, etc.)
        if "/" in v:
            raise ValueError("must not contain slashes")
        return v

    @validator("dsn")
    def __validate_dsn_(cls, value: str) -> str:
        try:
            psycopg.conninfo.conninfo_to_dict(value)
        except psycopg.ProgrammingError as e:
            raise ValueError(str(e)) from e
        return value
