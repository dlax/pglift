from pathlib import Path
from typing import Optional

import attr
from attr.validators import instance_of
from pgtoolkit import conf as pgconf
from pgtoolkit.conf import Configuration

from .settings import SETTINGS, Settings
from .validators import known_postgresql_version


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Instance:
    """A PostgreSQL instance."""

    name: str
    version: str = attr.ib(validator=known_postgresql_version)

    settings: Settings = attr.ib(default=SETTINGS, validator=instance_of(Settings))

    def __str__(self) -> str:
        """Return str(self).

        >>> i = Instance("main", "12")
        >>> str(i)
        '12/main'
        """
        return f"{self.version}/{self.name}"

    @property
    def path(self) -> Path:
        """Base directory path for this instance.

        >>> i = Instance("main", "12")
        >>> print(i.path)
        /var/lib/pgsql/12/main
        """
        pg_settings = self.settings.postgresql
        return pg_settings.root / pg_settings.instancedir.format(
            version=self.version, instance=self.name
        )

    @property
    def datadir(self) -> Path:
        """Path to data directory for this instance.

        >>> i = Instance("main", "12")
        >>> print(i.datadir)
        /var/lib/pgsql/12/main/data
        """
        return self.path / self.settings.postgresql.datadir

    @property
    def waldir(self) -> Path:
        """Path to WAL directory for this instance.

        >>> i = Instance("main", "12")
        >>> print(i.waldir)
        /var/lib/pgsql/12/main/wal
        """
        return self.path / self.settings.postgresql.waldir

    def config(self) -> Optional[Configuration]:
        """Return parsed PostgreSQL configuration for this instance, if it exists."""
        postgresql_conf = self.datadir / "postgresql.conf"
        if not postgresql_conf.exists():
            return None
        config = pgconf.parse(postgresql_conf)
        postgresql_auto_conf = self.datadir / "postgresql.auto.conf"
        if postgresql_auto_conf.exists():
            config += pgconf.parse(postgresql_auto_conf)
        return config

    def exists(self) -> bool:
        """Return True if the instance exists based on system lookup."""
        if not self.datadir.exists():
            return False
        if self.config() is None:
            return False
        real_version = (self.datadir / "PG_VERSION").read_text().splitlines()[0]
        if real_version != self.version:
            raise Exception(f"version mismatch ({real_version} != {self.version})")
        return True
