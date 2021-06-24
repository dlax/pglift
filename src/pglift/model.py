from pathlib import Path
from typing import Any, Optional

import attr
from attr.validators import instance_of
from pgtoolkit import conf as pgconf
from pgtoolkit.conf import Configuration

from . import conf, exceptions
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
class BaseInstance:

    name: str
    version: str = attr.ib(validator=known_postgresql_version)
    settings: Settings = attr.ib(validator=instance_of(Settings))
    prometheus: PrometheusService = attr.ib(factory=PrometheusService)

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
        return pg_settings.root / self.version / self.name

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

    def exists(self) -> bool:
        """Return True if the instance exists based on system lookup.

        :raises LookupError: if PG_VERSION content does not match declared version
        """
        if not self.datadir.exists():
            return False
        try:
            real_version = (self.datadir / "PG_VERSION").read_text().splitlines()[0]
        except FileNotFoundError:
            return False
        if real_version != self.version:
            raise LookupError(f"version mismatch ({real_version} != {self.version})")
        return True


@attr.s(auto_attribs=True, frozen=True, slots=True)
class InstanceSpec(BaseInstance):
    """Spec for an instance, to be created"""

    @classmethod
    def default_version(
        cls,
        name: str,
        ctx: BaseContext,
        *,
        prometheus: Optional[PrometheusService] = None,
    ) -> "InstanceSpec":
        """Build an instance by guessing its version from installed PostgreSQL."""
        settings = ctx.settings
        version = settings.postgresql.default_version
        if version is None:
            version = short_version(ctx.pg_ctl(None).version)
        extras = {}
        if prometheus is not None:
            extras["prometheus"] = prometheus
        return cls(name=name, version=version, settings=settings, **extras)

    @classmethod
    def from_stanza(
        cls, stanza: str, settings: Settings, **kwargs: Any
    ) -> "InstanceSpec":
        """Build an Instance from a '<version>-<name>' string.

        >>> s = Settings()
        >>> InstanceSpec.from_stanza('12-main', s)  # doctest: +ELLIPSIS
        InstanceSpec(name='main', version='12', ...)
        >>> InstanceSpec.from_stanza('bad', s)
        Traceback (most recent call last):
            ...
        ValueError: invalid stanza 'bad'
        """
        try:
            version, name = stanza.split("-", 1)
        except ValueError:
            raise ValueError(f"invalid stanza '{stanza}'") from None
        return cls(name, version, settings, **kwargs)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Instance(BaseInstance):
    """A PostgreSQL instance with satellite services"""

    @classmethod
    def from_spec(cls, spec: InstanceSpec) -> "Instance":
        """Build a (real) instance from a spec object."""
        instance = cls(
            **{k: getattr(spec, k) for k in attr.fields_dict(spec.__class__)}
        )
        try:
            instance.config()
        except Exception:
            raise exceptions.InstanceNotFound(str(instance))
        return instance

    def as_spec(self) -> InstanceSpec:
        return InstanceSpec(
            **{k: getattr(self, k) for k in attr.fields_dict(self.__class__)}
        )

    @classmethod
    def from_stanza(cls, stanza: str, **kwargs: Any) -> "Instance":
        """Build an Instance from a '<version>-<name>' string."""
        try:
            version, name = stanza.split("-", 1)
        except ValueError:
            raise ValueError(f"invalid stanza '{stanza}'") from None
        instance = cls(name, version, **kwargs)
        if not instance.exists():
            raise exceptions.InstanceNotFound(str(instance))
        return instance

    def exists(self) -> bool:
        """Return True if the instance exists and its configuration is valid.

        :raises ~pglift.exceptions.InstanceNotFound: if configuration cannot
            be read
        """
        if not super().exists():
            raise exceptions.InstanceNotFound(str(self))
        try:
            self.config()
        except FileNotFoundError:
            raise exceptions.InstanceNotFound(str(self))
        return True

    def config(self, managed_only: bool = False) -> Configuration:
        """Return parsed PostgreSQL configuration for this instance.

        If ``managed_only`` is ``True``, only the managed configuration is
        returned, otherwise the fully parsed configuration is returned.

        :raises FileNotFoundError: if expected configuration file is missing
        """
        if managed_only:
            confd = conf.info(self.datadir)[0]
            conffile = confd / "user.conf"
            return pgconf.parse(conffile)

        postgresql_conf = self.datadir / "postgresql.conf"
        config = pgconf.parse(postgresql_conf)
        postgresql_auto_conf = self.datadir / "postgresql.auto.conf"
        if postgresql_auto_conf.exists():
            config += pgconf.parse(postgresql_auto_conf)
        return config

    @property
    def port(self) -> int:
        """TCP port the server listens on."""
        return int(self.config().get("port", 5432))  # type: ignore[arg-type]
