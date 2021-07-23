from pathlib import Path
from typing import Optional, Tuple, Type, TypeVar, Union

import attr
from attr.validators import instance_of
from pgtoolkit.conf import Configuration

from .. import conf, exceptions
from ..ctx import BaseContext
from ..settings import Settings
from ..util import short_version
from ..validators import known_postgresql_version


def default_postgresql_version(ctx: BaseContext) -> str:
    version = ctx.settings.postgresql.default_version
    if version is None:
        version = short_version(ctx.pg_ctl(None).version)
    return version


@attr.s(auto_attribs=True, frozen=True, slots=True)
class PrometheusService:
    """A Prometheus postgres_exporter service bound to a PostgreSQL instance."""

    port: int = 9187
    """TCP port for the web interface and telemetry."""

    T = TypeVar("T", bound="PrometheusService")

    @classmethod
    def system_lookup(cls: Type[T], ctx: BaseContext, instance: "BaseInstance") -> T:
        from .. import prometheus

        return cls(port=prometheus.port(ctx, instance))


@attr.s(auto_attribs=True, frozen=True, slots=True)
class BaseInstance:

    name: str
    version: str = attr.ib(validator=known_postgresql_version)
    settings: Settings = attr.ib(validator=instance_of(Settings))

    def __str__(self) -> str:
        return f"{self.version}/{self.name}"

    @property
    def path(self) -> Path:
        """Base directory path for this instance."""
        pg_settings = self.settings.postgresql
        return pg_settings.root / self.version / self.name

    @property
    def datadir(self) -> Path:
        """Path to data directory for this instance."""
        return self.path / self.settings.postgresql.datadir

    @property
    def waldir(self) -> Path:
        """Path to WAL directory for this instance."""
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

    prometheus: PrometheusService = attr.ib(validator=instance_of(PrometheusService))

    T = TypeVar("T", bound="InstanceSpec")

    @classmethod
    def default_version(
        cls: Type[T], name: str, ctx: BaseContext, *, prometheus: PrometheusService
    ) -> T:
        """Build an instance by guessing its version from installed PostgreSQL."""
        version = default_postgresql_version(ctx)
        settings = ctx.settings
        return cls(name=name, version=version, settings=settings, prometheus=prometheus)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class PostgreSQLInstance(BaseInstance):
    """A bare PostgreSQL instance."""

    T = TypeVar("T", bound="PostgreSQLInstance")

    @classmethod
    def default_version(cls: Type[T], name: str, ctx: BaseContext) -> T:
        """Build an instance by guessing its version from installed PostgreSQL."""
        version = default_postgresql_version(ctx)
        settings = ctx.settings
        return cls(name=name, version=version, settings=settings)

    @classmethod
    def system_lookup(
        cls: Type[T],
        ctx: BaseContext,
        value: Union[BaseInstance, Tuple[str, Optional[str]]],
    ) -> T:
        """Build a (real) instance by system lookup.

        :param value: either a BaseInstance object or a (name, version) tuple.

        :raises ~exceptions.InstanceNotFound: if the instance could not be
            found by system lookup.
        """
        if not isinstance(value, BaseInstance):
            try:
                name, version = value
            except ValueError:
                raise TypeError(
                    "expecting either a BaseInstance or a (name, version) tuple as 'value' argument"
                )
        else:
            name, version = value.name, value.version
        if version is None:
            self = cls.default_version(name, ctx)
        else:
            self = cls(name, version, ctx.settings)
        if not self.exists():
            raise exceptions.InstanceNotFound(str(self))
        return self

    @classmethod
    def from_stanza(cls: Type[T], ctx: BaseContext, stanza: str) -> T:
        """Build an Instance from a '<version>-<name>' string."""
        try:
            version, name = stanza.split("-", 1)
        except ValueError:
            raise ValueError(f"invalid stanza '{stanza}'") from None
        return cls.system_lookup(ctx, (name, version))

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

        Refer to :func:`pglift.conf.read` for complete documentation.
        """
        return conf.read(self.datadir, managed_only=managed_only)

    @property
    def port(self) -> int:
        """TCP port the server listens on."""
        return int(self.config().get("port", 5432))  # type: ignore[arg-type]


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Instance(PostgreSQLInstance):
    """A PostgreSQL instance with satellite services."""

    prometheus: PrometheusService = attr.ib(validator=instance_of(PrometheusService))

    T = TypeVar("T", bound="Instance")

    @classmethod
    def system_lookup(
        cls: Type[T],
        ctx: BaseContext,
        value: Union[BaseInstance, Tuple[str, Optional[str]]],
    ) -> T:
        pg_instance = PostgreSQLInstance.system_lookup(ctx, value)
        values = attr.asdict(pg_instance)
        values["prometheus"] = PrometheusService.system_lookup(ctx, pg_instance)
        return cls(**values)

    def as_spec(self) -> InstanceSpec:
        return InstanceSpec(
            **{k: getattr(self, k) for k in attr.fields_dict(InstanceSpec)}
        )
