from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple

from pgtoolkit import conf as pgconf

from . import __name__ as pkgname
from . import exceptions

if TYPE_CHECKING:
    from .ctx import BaseContext
    from .models.system import BaseInstance


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


def create_log_directory(instance: "BaseInstance", path: Path) -> None:
    if not path.is_absolute():
        path = instance.datadir / path
    path.mkdir(parents=True, exist_ok=True)


def remove_log_directory(
    ctx: "BaseContext", instance: "BaseInstance", path: Path
) -> None:
    if not path.is_absolute():
        path = instance.datadir / path
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
