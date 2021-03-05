import functools
from pathlib import Path
from typing import Any

from pgtoolkit.conf import Configuration

from . import cmd


@functools.lru_cache(1)
def bindir(*, run_command: cmd.CommandRunner = cmd.run) -> Path:
    r = run_command(["pg_config", "--bindir"], check=True)
    return Path(r.stdout.strip())


@functools.lru_cache(512)
def binpath(name: str, *, run_command: cmd.CommandRunner = cmd.run) -> Path:
    return bindir(run_command=run_command) / name


def make_configuration(instance: str, **confitems: Any) -> Configuration:
    conf = Configuration()
    for key, value in confitems.items():
        conf[key] = value
    conf["cluster_name"] = instance
    return conf
