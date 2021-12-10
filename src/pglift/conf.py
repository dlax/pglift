import shutil
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional, Tuple, cast

from pgtoolkit import conf as pgconf

from . import __name__ as pkgname
from . import exceptions

if TYPE_CHECKING:
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


F = Callable[["BaseInstance", Path], None]


def absolute_path(fn: F) -> F:
    @wraps(fn)
    def wrapper(instance: "BaseInstance", path: Path) -> None:
        if not path.is_absolute():
            path = instance.datadir / path
        return fn(instance, path)

    return cast(F, wrapper)


@absolute_path
def create_log_directory(instance: "BaseInstance", path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@absolute_path
def remove_log_directory(instance: "BaseInstance", path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
