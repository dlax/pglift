from pathlib import Path
from typing import Any, Tuple

from pgtoolkit import conf as pgconf

from . import __name__ as pkgname


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
