import enum
import json
from pathlib import Path
from typing import IO, Any, Dict, Optional, Tuple, Type, TypeVar, Union

import yaml
from pgtoolkit.ctl import Status
from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    SecretStr,
    root_validator,
    validator,
)

from .. import settings
from ..ctx import BaseContext
from ..types import AutoStrEnum
from . import system as system_model


class InstanceState(AutoStrEnum):
    """Instance state."""

    stopped = enum.auto()
    """stopped"""

    started = enum.auto()
    """started"""

    absent = enum.auto()
    """absent"""

    @classmethod
    def from_pg_status(cls, status: Status) -> "InstanceState":
        """Instance state from PostgreSQL status.

        >>> InstanceState.from_pg_status(Status.running)
        <InstanceState.started: 'started'>
        >>> InstanceState.from_pg_status(Status.not_running)
        <InstanceState.stopped: 'stopped'>
        >>> InstanceState.from_pg_status(Status.unspecified_datadir)
        <InstanceState.absent: 'absent'>
        """
        return cls(
            {
                status.running: cls.started,
                status.not_running: cls.stopped,
                status.unspecified_datadir: cls.absent,
            }[status]
        )


class InstanceListItem(BaseModel):

    name: str
    version: str
    port: int
    path: DirectoryPath
    status: str


T = TypeVar("T", bound=BaseModel)


class Manifest(BaseModel):
    """Base class for manifest data classes."""

    class Config:
        extra = "forbid"

    @classmethod
    def parse_yaml(cls: Type[T], stream: IO[str]) -> T:
        """Parse from a YAML stream."""
        data = yaml.safe_load(stream)
        return cls.parse_obj(data)

    def yaml(self, **kwargs: Any) -> str:
        """Return a YAML serialization of this manifest."""
        data = json.loads(self.json(**kwargs))
        return yaml.dump(data, sort_keys=False)  # type: ignore[no-any-return]


class Instance(Manifest):
    """PostgreSQL instance"""

    class Prometheus(BaseModel):
        port: int = Field(
            default=9187,
            description="TCP port for the web interface and telemetry of Prometheus",
        )

    name: str
    version: Optional[str] = Field(default=None, description="PostgreSQL version")
    port: Optional[int] = Field(
        default=None,
        description="TCP port the postgresql instance will be listening to",
    )
    state: InstanceState = Field(
        default=InstanceState.started,
        description="Runtime state",
        cli={"choices": [InstanceState.started.value, InstanceState.stopped.value]},
    )
    ssl: Union[bool, Tuple[Path, Path]] = Field(
        default=False,
        cli={"hide": True},
        ansible={"spec": {"type": "bool", "required": False, "default": False}},
    )
    configuration: Dict[str, Any] = Field(
        default_factory=dict,
        cli={"hide": True},
        ansible={"spec": {"type": "dict", "required": False}},
    )

    prometheus: Prometheus = Prometheus()

    @validator("name")
    def __validate_name_(cls, v: str) -> str:
        """Validate 'name' field.

        >>> Instance(name='without_dash')  # doctest: +ELLIPSIS
        Instance(name='without_dash', ...)
        >>> Instance(name='with-dash')
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        name
          instance name must not contain dashes (type=value_error)
        """
        # Avoid dash as this will break systemd instance unit.
        if "-" in v:
            raise ValueError("instance name must not contain dashes")
        return v

    @validator("version")
    def __validate_version_(cls, v: Optional[str]) -> Optional[str]:
        """Validate 'version' field.

        >>> Instance(name="x", version=None).version
        >>> Instance(name="x", version="13").version
        '13'
        >>> Instance(name="x", version="9")
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        version
          unsupported PostgreSQL version: 9 (type=value_error)
        """
        if v is None:
            return None
        if v not in settings.POSTGRESQL_SUPPORTED_VERSIONS:
            raise ValueError(f"unsupported PostgreSQL version: {v}")
        return v

    @root_validator
    def __port_not_in_configuration_(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that 'configuration' field has no 'port' key.

        >>> Instance(name="i")
        Instance(name='i', ...)
        >>> Instance(name="i", configuration={"port": 123})
        Traceback (most recent call last):
            ...
        pydantic.error_wrappers.ValidationError: 1 validation error for Instance
        __root__
          port should not be specified in configuration field (type=value_error)
        """
        if "port" in values.get("configuration", {}):
            raise ValueError("port should not be specified in configuration field")
        return values

    def spec(self, ctx: BaseContext) -> system_model.InstanceSpec:
        """Return an InstanceSpec matching this manifest."""
        prometheus = system_model.PrometheusService(port=self.prometheus.port)
        if self.version is not None:
            return system_model.InstanceSpec(
                self.name,
                self.version,
                settings=ctx.settings,
                prometheus=prometheus,
            )
        else:
            return system_model.InstanceSpec.default_version(
                self.name,
                ctx,
                prometheus=prometheus,
            )


class Role(Manifest):
    """PostgreSQL role"""

    class State(AutoStrEnum):
        present = enum.auto()
        absent = enum.auto()

    name: str
    password: Optional[SecretStr] = Field(default=None, description="role password")
    pgpass: bool = Field(
        default=False, description="add an entry in password file for this role"
    )
    inherit: bool = Field(
        default=True,
        description="let the role inherits the privileges of the roles its is a member of",
    )
    login: bool = Field(default=False, description="allow the role to log in")
    state: State = Field(default=State.present, cli={"hide": True})


class Database(Manifest):
    """PostgreSQL database"""

    class State(AutoStrEnum):
        present = enum.auto()
        absent = enum.auto()

    name: str
    state: State = Field(default=State.present, cli={"hide": True})
