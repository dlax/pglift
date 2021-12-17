from pathlib import Path
from typing import Optional, Tuple, Type, TypeVar, Union

import attr
from attr.validators import instance_of
from pgtoolkit.conf import Configuration

from .. import conf, exceptions, logger
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

    port: int
    """TCP port for the web interface and telemetry."""

    T = TypeVar("T", bound="PrometheusService")

    @classmethod
    def system_lookup(
        cls: Type[T], ctx: BaseContext, instance: "BaseInstance"
    ) -> Optional[T]:
        from .. import prometheus

        try:
            port = prometheus.port(ctx, instance.qualname)
        except exceptions.FileNotFoundError as exc:
            logger.debug(
                "failed to read postgres_exporter port for %s: %s", instance, exc
            )
            return None
        else:
            return cls(port)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class BaseInstance:

    name: str
    version: str = attr.ib(validator=known_postgresql_version)
    settings: Settings = attr.ib(validator=instance_of(Settings))

    T = TypeVar("T", bound="BaseInstance")

    def __str__(self) -> str:
        return f"{self.version}/{self.name}"

    @property
    def qualname(self) -> str:
        """Version qualified name, e.g. 13-main."""
        return f"{self.version}-{self.name}"

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

        :raises ~exceptions.InvalidVersion: if PG_VERSION content does not
            match declared version
        """
        if not self.datadir.exists():
            return False
        try:
            real_version = (self.datadir / "PG_VERSION").read_text().splitlines()[0]
        except FileNotFoundError:
            return False
        if real_version != self.version:
            raise exceptions.InvalidVersion(
                f"version mismatch ({real_version} != {self.version})"
            )
        return True

    @classmethod
    def get(cls: Type[T], name: str, version: Optional[str], ctx: BaseContext) -> T:
        return cls(name, version or default_postgresql_version(ctx), ctx.settings)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Standby:
    for_: str
    slot: Optional[str]

    T = TypeVar("T", bound="Standby")

    @classmethod
    def system_lookup(cls: Type[T], instance: "PostgreSQLInstance") -> Optional[T]:
        standbyfile = (
            "standby.signal" if int(instance.version) >= 12 else "recovery.conf"
        )
        if not (instance.datadir / standbyfile).exists():
            return None
        # primary_conninfo must be present here, otherwise this is considered
        # as an error
        config = instance.config()
        primary_conninfo = config["primary_conninfo"]
        assert isinstance(primary_conninfo, str)
        slot = config.get("primary_slot_name")
        if slot is not None:
            assert isinstance(slot, str), slot
        return cls(for_=primary_conninfo, slot=slot or None)


@attr.s(auto_attribs=True, frozen=True, slots=True)
class PostgreSQLInstance(BaseInstance):
    """A bare PostgreSQL instance."""

    T = TypeVar("T", bound="PostgreSQLInstance")

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
        self = cls.get(name, version, ctx)
        if not self.exists():
            raise exceptions.InstanceNotFound(str(self))
        return self

    @property
    def standby(self) -> Optional[Standby]:
        return Standby.system_lookup(self)

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
        except FileNotFoundError as e:
            raise exceptions.InstanceNotFound(str(self)) from e
        return True

    def config(self, managed_only: bool = False) -> Configuration:
        """Return parsed PostgreSQL configuration for this instance.

        Refer to :func:`pglift.conf.read` for complete documentation.
        """
        try:
            return conf.read(self.datadir, managed_only=managed_only)
        except exceptions.FileNotFoundError:
            if managed_only:
                return Configuration()
            raise

    @property
    def port(self) -> int:
        """TCP port the server listens on."""
        return int(self.config().get("port", 5432))  # type: ignore[arg-type]


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Instance(PostgreSQLInstance):
    """A PostgreSQL instance with satellite services."""

    prometheus: Optional[PrometheusService] = attr.ib(
        validator=instance_of((type(None), PrometheusService))
    )

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
