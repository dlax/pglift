from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, TypeVar, Union

import attr
from attr.validators import instance_of
from pgtoolkit.conf import Configuration

from .. import conf, exceptions
from ..settings import Settings
from ..util import short_version
from ..validators import known_postgresql_version

if TYPE_CHECKING:
    from ..ctx import BaseContext


def default_postgresql_version(ctx: "BaseContext") -> str:
    version = ctx.settings.postgresql.default_version
    if version is None:
        version = short_version(ctx.pg_ctl(None).version)
    return version


@attr.s(auto_attribs=True, frozen=True, slots=True)
class BaseInstance:

    name: str
    version: str = attr.ib(validator=known_postgresql_version)

    _settings: Settings = attr.ib(validator=instance_of(Settings))

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
        pg_settings = self._settings.postgresql
        return pg_settings.root / self.version / self.name

    @property
    def datadir(self) -> Path:
        """Path to data directory for this instance."""
        return self.path / self._settings.postgresql.datadir

    @property
    def waldir(self) -> Path:
        """Path to WAL directory for this instance."""
        return self.path / self._settings.postgresql.waldir

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
    def get(cls: Type[T], name: str, version: Optional[str], ctx: "BaseContext") -> T:
        # attrs strip leading underscores at init for private attributes.
        return cls(
            name, version or default_postgresql_version(ctx), settings=ctx.settings
        )


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
        ctx: "BaseContext",
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
    def from_stanza(cls: Type[T], ctx: "BaseContext", stanza: str) -> T:
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

    services: List[Any] = attr.ib()

    @services.validator
    def _validate_services(self, attribute: Any, value: List[Any]) -> None:
        if len(set(map(type, value))) != len(value):
            raise ValueError("values for 'services' field must be of distinct types")

    T = TypeVar("T", bound="Instance")

    @classmethod
    def system_lookup(
        cls: Type[T],
        ctx: "BaseContext",
        value: Union[BaseInstance, Tuple[str, Optional[str]]],
    ) -> T:
        pg_instance = PostgreSQLInstance.system_lookup(ctx, value)
        values = attr.asdict(pg_instance)
        # attrs strip leading underscores at init for private attributes.
        values["settings"] = values.pop("_settings")
        assert "services" not in values
        values["services"] = [
            s
            for s in ctx.hook.system_lookup(ctx=ctx, instance=pg_instance)
            if s is not None
        ]
        return cls(**values)

    S = TypeVar("S")

    def service(self, stype: Type[S]) -> S:
        """Return bound satellite service object matching requested type.

        :raises ValueError: if not found.
        """
        for s in self.services:
            if isinstance(s, stype):
                return s
        raise ValueError(stype)
