import functools
import subprocess
from pathlib import Path
from typing import Any

from pgtoolkit.conf import Configuration


@functools.lru_cache(1)
def bindir() -> Path:
    out = subprocess.check_output(["pg_config", "--bindir"], universal_newlines=True)
    return Path(out.strip())


@functools.lru_cache(512)
def binpath(name: str) -> Path:
    return bindir() / name


def make_configuration(instance: str, **confitems: Any) -> Configuration:
    conf = Configuration()
    for key, value in confitems.items():
        conf[key] = value
    conf["cluster_name"] = instance
    return conf
