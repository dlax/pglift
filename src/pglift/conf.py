import shutil
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Tuple, cast

from pgtoolkit import conf as pgconf

from . import __name__ as pkgname

if TYPE_CHECKING:
    from .model import Instance


def make(instance: str, **confitems: Any) -> pgconf.Configuration:
    """Return a :class:`pgtoolkit.conf.Configuration` for named `instance`
    filled with given items.
    """
    conf = pgconf.Configuration()
    conf["cluster_name"] = instance
    for key, value in confitems.items():
        conf[key] = value
    return conf


def info(configdir: Path, name: str = "user.conf") -> Tuple[Path, Path, str]:
    """Return (confd, conffile, include) where `confd` is the path to
    directory where managed configuration files live; `conffile` is the path
    configuration file `name` and `include` is an include directive to be
    inserted in main 'postgresql.conf'.
    """
    confd = Path(f"conf.{pkgname}.d")
    include = f"include_dir = '{confd}'"
    confd = configdir / confd
    conffile = confd / name
    return confd, conffile, include


F = Callable[["Instance", Path], None]


def absolute_path(fn: F) -> F:
    @wraps(fn)
    def wrapper(instance: "Instance", path: Path) -> None:
        if not path.is_absolute():
            path = instance.datadir / path
        return fn(instance, path)

    return cast(F, wrapper)


@absolute_path
def create_log_directory(instance: "Instance", path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@absolute_path
def remove_log_directory(instance: "Instance", path: Path) -> None:
    shutil.rmtree(path)
