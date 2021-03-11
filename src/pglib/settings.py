import functools
import json
from pathlib import Path
from typing import Any, List, Optional

import attr
import environ
from attr.validators import instance_of


@environ.config(frozen=True)
class PostgreSQLSettings:
    """Settings for PostgreSQL."""

    versions: List[str] = environ.var(
        default=attr.Factory(lambda: ["13", "12", "11", "10", "9.6"])
    )
    """Available PostgreSQL versions."""

    root: Path = environ.var(
        default=Path("/var/lib/pgsql"), validator=instance_of(Path), converter=Path
    )
    """Root directory for all managed instances."""

    locale: Optional[str] = environ.var(default="C")
    """Instance locale as used by initdb."""

    surole: str = environ.var(default="postgres")
    """User name of instance super-user."""

    instancedir: str = environ.var(default="{version}/{instance}")
    """Path segment to instance base directory relative to `root` path."""

    datadir: str = environ.var(default="data")
    """Path segment from instance base directory to PGDATA directory."""

    waldir: str = environ.var("wal")
    """Path segment from instance base directory to WAL directory."""


@environ.config(prefix="PGLIB", frozen=True)
class Settings:

    postgresql: PostgreSQLSettings = environ.group(PostgreSQLSettings)


to_config = functools.partial(environ.to_config, Settings)

SETTINGS = to_config()


if __name__ == "__main__":

    def default(obj: Any) -> Any:
        if isinstance(obj, Path):
            return str(obj)
        return obj

    print(json.dumps(attr.asdict(SETTINGS), indent=2, default=default))
