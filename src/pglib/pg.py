import functools
import subprocess
from pathlib import Path


@functools.lru_cache(1)
def bindir() -> Path:
    out = subprocess.check_output(["pg_config", "--bindir"], universal_newlines=True)
    return Path(out.strip())


@functools.lru_cache(512)
def binpath(name: str) -> Path:
    return bindir() / name
