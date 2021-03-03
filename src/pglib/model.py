from pathlib import Path

import attr
from attr.validators import instance_of
from pgtoolkit import conf as pgconf

from .settings import SETTINGS, Settings
from .validators import known_postgresql_version


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Instance:
    """A PostgreSQL instance."""

    name: str
    version: str = attr.ib(validator=known_postgresql_version)
    port: int = attr.ib(validator=instance_of(int))

    settings: Settings = attr.ib(default=SETTINGS, validator=instance_of(Settings))

    def __str__(self) -> str:
        """Return str(self).

        >>> i = Instance("main", "12", 5432)
        >>> str(i)
        '12/main'
        """
        return f"{self.version}/{self.name}"

    @property
    def path(self) -> Path:
        """Base directory path for this instance.

        >>> i = Instance("main", "12", 5432)
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

        >>> i = Instance("main", "12", 5432)
        >>> print(i.datadir)
        /var/lib/pgsql/12/main/data
        """
        return self.path / self.settings.postgresql.datadir

    @property
    def waldir(self) -> Path:
        """Path to WAL directory for this instance.

        >>> i = Instance("main", "12", 5432)
        >>> print(i.waldir)
        /var/lib/pgsql/12/main/wal
        """
        return self.path / self.settings.postgresql.waldir

    def exists(self) -> bool:
        """Return True if the instance exists based on system lookup."""
        if not self.datadir.exists():
            return False
        postgresql_conf = self.datadir / "postgresql.conf"
        if not postgresql_conf.exists():
            return False
        config = pgconf.parse(postgresql_conf)
        postgresql_auto_conf = self.datadir / "postgresql.auto.conf"
        if postgresql_auto_conf.exists():
            config += pgconf.parse(postgresql_auto_conf)
        if "port" in config:
            if config.port != self.port:
                raise Exception(f"port mismatch ({config.port} != {self.port})")
        real_version = (self.datadir / "PG_VERSION").read_text().splitlines()[0]
        if real_version != self.version:
            raise Exception(f"version mismatch ({real_version} != {self.version})")
        return True
