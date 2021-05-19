from pathlib import Path
from typing import Any, Optional

import attr
from attr.validators import instance_of
from pgtoolkit import conf as pgconf
from pgtoolkit.conf import Configuration

from . import conf
from .ctx import BaseContext
from .settings import SETTINGS, Settings
from .util import short_version
from .validators import known_postgresql_version


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Instance:
    """A PostgreSQL instance."""

    name: str
    version: str = attr.ib(validator=known_postgresql_version)

    settings: Settings = attr.ib(default=SETTINGS, validator=instance_of(Settings))

    @classmethod
    def from_stanza(cls, stanza: str, **kwargs: Any) -> "Instance":
        """Build an Instance from a '<version>-<name>' string.

        >>> Instance.from_stanza('9.6-main')  # doctest: +ELLIPSIS
        Instance(name='main', version='9.6', ...)
        >>> Instance.from_stanza('bad')
        Traceback (most recent call last):
            ...
        ValueError: invalid stanza 'bad'
        """
        try:
            version, name = stanza.split("-", 1)
        except ValueError:
            raise ValueError(f"invalid stanza '{stanza}'") from None
        return cls(name, version, **kwargs)

    @classmethod
    def default_version(cls, name: str, ctx: BaseContext, **kwargs: Any) -> "Instance":
        """Build an Instance by guessing its version from installed PostgreSQL."""
        try:
            settings = kwargs.pop("settings")
        except KeyError:
            settings = ctx.settings
        else:
            assert (
                settings == ctx.settings
            ), "settings bound to context is inconsistent with passed value"
        version = settings.postgresql.default_version
        if version is None:
            version = short_version(ctx.pg_ctl(None).version)
        return cls(name, version, settings=settings)

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

        >>> i = Instance("main", "12")
        >>> print(i.datadir)  # doctest: +ELLIPSIS
        /.../srv/pgsql/12/main/data
        """
        return self.path / self.settings.postgresql.datadir

    @property
    def waldir(self) -> Path:
        """Path to WAL directory for this instance.

        >>> i = Instance("main", "12")
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
        real_version = (self.datadir / "PG_VERSION").read_text().splitlines()[0]
        if real_version != self.version:
            raise Exception(f"version mismatch ({real_version} != {self.version})")
        return True
