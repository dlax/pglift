import enum
import json
from pathlib import Path
from typing import IO, Any, Dict, Optional, Tuple, Type, TypeVar, Union

import yaml
from pgtoolkit.ctl import Status
from pydantic import BaseModel, Field

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

    name: str
    version: Optional[str] = None
    state: InstanceState = InstanceState.started
    ssl: Union[bool, Tuple[Path, Path]] = False
    configuration: Dict[str, Any] = Field(default_factory=dict)

    def model(self, ctx: BaseContext) -> model.Instance:
        """Return a model Instance matching this manifest."""
        if self.version is not None:
            return model.Instance(self.name, self.version, settings=ctx.settings)
        else:
            return model.Instance.default_version(self.name, ctx)
