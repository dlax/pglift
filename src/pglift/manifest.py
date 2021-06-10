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

from . import model
from .ctx import BaseContext


@enum.unique
class InstanceState(enum.Enum):
    """Instance state."""

    stopped = "stopped"
    """stopped"""

    started = "started"
    """started"""

    absent = "absent"
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
                status.running: "started",
                status.not_running: "stopped",
                status.unspecified_datadir: "absent",
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

    def yaml(self) -> str:
        """Return a YAML serialization of this manifest."""
        data = json.loads(self.json(exclude_defaults=True))
        return yaml.dump(data, sort_keys=False)  # type: ignore[no-any-return]


class Instance(Manifest):
    """PostgreSQL instance"""

    class Prometheus(BaseModel):
        port: int = 9187
        """TCP port for the web interface and telemetry."""

    name: str
    version: Optional[str] = None
    port: Optional[int] = None
    state: InstanceState = InstanceState.started
    ssl: Union[bool, Tuple[Path, Path]] = False
    configuration: Dict[str, Any] = Field(default_factory=dict)

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

    def model(self, ctx: BaseContext) -> model.InstanceSpec:
        """Return a model Instance matching this manifest."""
        if self.version is not None:
            return model.InstanceSpec(self.name, self.version, settings=ctx.settings)
        else:
            return model.InstanceSpec.default_version(self.name, ctx)


class Role(Manifest):
    """PostgreSQL role"""

    name: str
    password: Optional[SecretStr] = None
    pgpass: bool = False
