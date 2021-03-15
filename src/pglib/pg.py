import functools
from pathlib import Path

from .types import CommandRunner


@functools.lru_cache(1)
def bindir(*, run_command: CommandRunner) -> Path:
    r = run_command(["pg_config", "--bindir"], check=True)
    return Path(r.stdout.strip())


@functools.lru_cache(512)
def binpath(name: str, *, run_command: CommandRunner) -> Path:
    return bindir(run_command=run_command) / name
