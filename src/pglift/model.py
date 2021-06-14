from pathlib import Path
from typing import Any, Optional

import attr
from attr.validators import instance_of
from pgtoolkit import conf as pgconf
from pgtoolkit.conf import Configuration

from . import conf
from .ctx import BaseContext
from .settings import Settings
from .util import short_version
from .validators import known_postgresql_version


@attr.s(auto_attribs=True, frozen=True, slots=True)
class PrometheusService:
    """A Prometheus postgres_exporter service bound to a PostgreSQL instance."""

    port: int = 9187
    """TCP port for the web interface and telemetry."""


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Instance:
    """A PostgreSQL instance."""

    name: str
    version: str = attr.ib(validator=known_postgresql_version)
    settings: Settings = attr.ib(validator=instance_of(Settings))
    prometheus: PrometheusService = attr.ib(factory=PrometheusService)

    @classmethod
    def from_stanza(cls, stanza: str, settings: Settings, **kwargs: Any) -> "Instance":
        """Build an Instance from a '<version>-<name>' string.

        >>> s = Settings()
        >>> Instance.from_stanza('12-main', s)  # doctest: +ELLIPSIS
        Instance(name='main', version='12', ...)
        >>> Instance.from_stanza('bad', s)
        Traceback (most recent call last):
            ...
        ValueError: invalid stanza 'bad'
        """
        try:
            version, name = stanza.split("-", 1)
        except ValueError:
            raise ValueError(f"invalid stanza '{stanza}'") from None
        return cls(name, version, settings, **kwargs)

    @classmethod
    def default_version(
        cls,
        name: str,
        ctx: BaseContext,
        *,
        prometheus: Optional[PrometheusService] = None,
    ) -> "Instance":
        """Build an Instance by guessing its version from installed PostgreSQL."""
        settings = ctx.settings
        version = settings.postgresql.default_version
        if version is None:
            version = short_version(ctx.pg_ctl(None).version)
        extras = {}
        if prometheus is not None:
            extras["prometheus"] = prometheus
        return cls(name=name, version=version, settings=settings, **extras)

    def __str__(self) -> str:
        """Return str(self).

        >>> i = Instance("main", "12", Settings())
        >>> str(i)
        '12/main'
        """
        return f"{self.version}/{self.name}"

    @property
    def path(self) -> Path:
        """Base directory path for this instance.

        >>> i = Instance("main", "12", Settings())
        >>> print(i.path)  # doctest: +ELLIPSIS
        /.../srv/pgsql/12/main
        """
        pg_settings = self.settings.postgresql
        return pg_settings.root / pg_settings.instancedir.format(
            version=self.version, instance=self.name
        )

    @property
    def datadir(self) -> Path:
        """Path to data directory for this instance.

        >>> i = Instance("main", "12", Settings())
        >>> print(i.datadir)  # doctest: +ELLIPSIS
        /.../srv/pgsql/12/main/data
        """
        return self.path / self.settings.postgresql.datadir

    @property
    def waldir(self) -> Path:
        """Path to WAL directory for this instance.

        >>> i = Instance("main", "12", Settings())
        >>> print(i.waldir)  # doctest: +ELLIPSIS
        /.../srv/pgsql/12/main/wal
        """
        return self.path / self.settings.postgresql.waldir

    def config(self, managed_only: bool = False) -> Optional[Configuration]:
        """Return parsed PostgreSQL configuration for this instance, if it
        exists.

        If ``managed_only`` is ``True``, only the managed configuration is
        returned, otherwise the fully parsed configuration is returned.
        """
        if managed_only:
            conffile = conf.info(self.datadir)[1]
            if not conffile.exists():
                return None
            return pgconf.parse(conffile)

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
        try:
            real_version = (self.datadir / "PG_VERSION").read_text().splitlines()[0]
        except FileNotFoundError:
            return False
        if real_version != self.version:
            raise Exception(f"version mismatch ({real_version} != {self.version})")
        return True
