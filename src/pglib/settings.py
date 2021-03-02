from pathlib import Path
from typing import List, Optional

import attr
from attr.validators import instance_of


@attr.s(auto_attribs=True, frozen=True, slots=True)
class PostgreSQLSettings:
    """Settings for PostgreSQL."""

    versions: List[str] = attr.ib(
        default=attr.Factory(lambda: ["13", "12", "11", "10", "9.6"])
    )
    """Available PostgreSQL versions."""

    root: Path = attr.ib(default=Path("/var/lib/pgsql"), validator=instance_of(Path))
    """Root directory for all managed instances."""

    locale: Optional[str] = "C"
    """Instance locale as used by initdb."""

    surole: str = "postgres"
    """User name of instance super-user."""

    config_file: str = "postgresql.pglib.conf"
    """Name of file containing managed configuration entries."""

    instancedir: str = "{version}/{instance}"
    """Path segment to instance base directory relative to `root` path."""

    datadir: str = "data"
    """Path segment from instance base directory to PGDATA directory."""

    waldir: str = "wal"
    """Path segment from instance base directory to WAL directory."""


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Settings:
    postgresql: PostgreSQLSettings

    @classmethod
    def load(cls) -> "Settings":
        """Instantiate a Settings object from available data sources."""
        return cls(postgresql=PostgreSQLSettings())


SETTINGS = Settings.load()
