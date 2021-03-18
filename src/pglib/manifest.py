import enum
from pathlib import Path
from typing import IO, Any, Dict, Optional, Tuple, Type, TypeVar, Union

import yaml
from pydantic import BaseModel, Field

from . import model
from .ctx import BaseContext
from .settings import SETTINGS, Settings


@enum.unique
class InstanceState(enum.Enum):
    """Instance state."""

    stopped = "stopped"
    """stopped"""

    started = "started"
    """started"""

    absent = "absent"
    """absent"""


T = TypeVar("T", bound=BaseModel)


class Manifest(BaseModel):
    """Base class for manifest data classes."""

    @classmethod
    def parse_yaml(cls: Type[T], stream: IO[str]) -> T:
        """Parse from a YAML stream."""
        data = yaml.safe_load(stream)
        return cls.parse_obj(data)


class Instance(Manifest):
    """PostgreSQL instance"""

    name: str
    version: Optional[str] = None
    state: InstanceState = InstanceState.started
    ssl: Union[bool, Tuple[Path, Path]] = False
    configuration: Dict[str, Any] = Field(default_factory=dict)

    def model(
        self, ctx: BaseContext, *, settings: Settings = SETTINGS
    ) -> model.Instance:
        """Return a model Instance matching this manifest."""
        if self.version is not None:
            return model.Instance(self.name, self.version, settings=settings)
        else:
            return model.Instance.default_version(self.name, ctx, settings=settings)
