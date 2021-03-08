import enum
import functools
from pathlib import Path
from typing import Any

from pgtoolkit.conf import Configuration

from . import cmd
from .types import CommandRunner, CompletedProcess


@enum.unique
class Status(enum.IntEnum):
    RUNNING = 0
    NOT_RUNNING = 3
    UNSPECIFIED_DATADIR = 4


@functools.lru_cache(1)
def bindir(*, run_command: CommandRunner = cmd.run) -> Path:
    r = run_command(["pg_config", "--bindir"], check=True)
    return Path(r.stdout.strip())


@functools.lru_cache(512)
def binpath(name: str, *, run_command: CommandRunner = cmd.run) -> Path:
    return bindir(run_command=run_command) / name


def make_configuration(instance: str, **confitems: Any) -> Configuration:
    conf = Configuration()
    for key, value in confitems.items():
        conf[key] = value
    conf["cluster_name"] = instance
    return conf


def ctl(
    datadir: Path,
    *args: str,
    run_command: CommandRunner = cmd.run,
    **kwargs: Any,
) -> CompletedProcess:
    pg_ctl = binpath("pg_ctl", run_command=run_command)
    return run_command([str(pg_ctl), "-D", str(datadir)] + list(args), **kwargs)
