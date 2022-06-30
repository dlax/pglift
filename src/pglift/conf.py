from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from pgtoolkit import conf as pgconf

from . import __name__ as pkgname
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


def info(configdir: Path) -> Tuple[Path, str]:
    """Return (confd, include) where `confd` is the path to
    directory where managed configuration files live and `include` is an
    include directive to be inserted in main 'postgresql.conf'.
    """
    confd = Path(f"conf.{pkgname}.d")
    include = f"include_dir = '{confd}'"
    confd = configdir / confd
    return confd, include


def read(configdir: Path, managed_only: bool = False) -> pgconf.Configuration:
    """Return parsed PostgreSQL configuration for given `configdir`.

    If ``managed_only`` is ``True``, only the managed configuration is
    returned, otherwise the fully parsed configuration is returned.

    :raises ~exceptions.FileNotFoundError: if expected configuration file is missing.
    """

    def conffile_notfound(path: Path) -> exceptions.FileNotFoundError:
        return exceptions.FileNotFoundError(
            f"PostgreSQL configuration file {path} not found"
        )

    if managed_only:
        confd = info(configdir)[0]
        conffile = confd / "user.conf"
        if not conffile.exists():
            raise conffile_notfound(conffile)
        return pgconf.parse(conffile)

    postgresql_conf = configdir / "postgresql.conf"
    if not postgresql_conf.exists():
        raise conffile_notfound(postgresql_conf)
    config = pgconf.parse(postgresql_conf)

    for extra_conf in ("postgresql.auto.conf", "recovery.conf"):
        try:
            config += pgconf.parse(configdir / extra_conf)
        except FileNotFoundError:
            pass
    return config


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
