from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

from pgtoolkit import conf as pgconf

from . import exceptions, util

if TYPE_CHECKING:
    from .ctx import BaseContext
    from .settings import PostgreSQLSettings


def make(instance: str, **confitems: Optional[pgconf.Value]) -> pgconf.Configuration:
    """Return a :class:`pgtoolkit.conf.Configuration` for named `instance`
    filled with given items.
    """
    conf = pgconf.Configuration()
    for key, value in confitems.items():
        if value is not None:
            conf[key] = value
    return conf


def read(configdir: Path, managed_only: bool = False) -> pgconf.Configuration:
    """Return parsed PostgreSQL configuration for given `configdir`.

    If ``managed_only`` is ``True``, only the managed configuration is
    returned excluding 'postgresql.auto.conf' or 'recovery.conf', otherwise
    the fully parsed configuration is returned.

    :raises ~exceptions.FileNotFoundError: if expected configuration file is missing.
    """
    postgresql_conf = configdir / "postgresql.conf"
    if not postgresql_conf.exists():
        raise exceptions.FileNotFoundError(
            f"PostgreSQL configuration file {postgresql_conf} not found"
        )
    config = pgconf.parse(postgresql_conf)

    if managed_only:
        return config

    for extra_conf in ("postgresql.auto.conf", "recovery.conf"):
        try:
            config += pgconf.parse(configdir / extra_conf)
        except FileNotFoundError:
            pass
    return config


def update(base: pgconf.Configuration, **values: pgconf.Value) -> None:
    """Update 'base' configuration so that it contains new values.

    Entries absent from 'values' but present in 'base' are commented out.
    """
    with base.edit() as entries:
        for key, value in list(entries.items()):
            if value.commented:
                continue
            try:
                new = values.pop(key)
            except KeyError:
                entries[key].commented = True
            else:
                entries[key].value = new
                entries[key].commented = False
        for key, val in values.items():
            try:
                entries[key].value = val
                entries[key].commented = False
            except KeyError:
                entries.add(key, val)


def log_directory(datadir: Path, path: Path) -> Path:
    if not path.is_absolute():
        path = datadir / path
    return path


def remove_log_directory(ctx: "BaseContext", datadir: Path, path: Path) -> None:
    path = log_directory(datadir, path)
    if path.exists():
        ctx.rmtree(path)


def merge_lists(first: str, second: str) -> str:
    """Contatenate two coma separated lists eliminating duplicates.

    >>> old = ""
    >>> new = "foo"
    >>> merge_lists(old, new)
    'foo'

    >>> old = "foo, bar, dude"
    >>> new = "bar, truite"
    >>> merge_lists(old, new)
    'foo, bar, dude, truite'
    """
    first_list = [s.strip() for s in first.split(",") if s.strip()]
    second_list = [s.strip() for s in second.split(",") if s.strip()]
    return ", ".join(first_list + [s for s in second_list if s not in first_list])


def format_values(
    confitems: Dict[str, Any],
    settings: "PostgreSQLSettings",
    memtotal: float = util.total_memory(),
) -> None:
    for k in ("shared_buffers", "effective_cache_size"):
        try:
            v = confitems[k]
        except KeyError:
            continue
        if v is None:
            continue
        try:
            confitems[k] = util.percent_memory(v, memtotal)
        except ValueError:
            pass
    for k, v in confitems.items():
        if isinstance(v, str):
            confitems[k] = v.format(settings=settings)
