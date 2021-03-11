from pathlib import Path
from typing import Tuple


def info(configdir: Path, name: str = "user.conf") -> Tuple[Path, Path, str]:
    """Return (confd, conffile, include) where `confd` is the path to
    directory where managed configuration files live; `conffile` is the path
    configuration file `name` and `include` is an include directive to be
    inserted in main 'postgresql.conf'.
    """
    confd = Path("pglib.conf.d")
    include = f"include_dir = '{confd}'"
    confd = configdir / confd
    conffile = confd / name
    return confd, conffile, include
