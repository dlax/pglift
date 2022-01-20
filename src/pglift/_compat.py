import shlex
import sys
from typing import Iterable

pyversion = sys.version_info[:2]

if pyversion >= (3, 8):
    shlex_join = shlex.join  # type: ignore[attr-defined]
else:

    def shlex_join(split_command: Iterable[str]) -> str:
        return " ".join(shlex.quote(arg) for arg in split_command)


if pyversion >= (3, 8):
    from importlib.metadata import version
else:
    from importlib_metadata import version  # type: ignore[no-redef]


if pyversion >= (3, 7):
    from contextlib import nullcontext
else:
    from contextlib2 import nullcontext  # type: ignore[no-redef]


__all__ = [
    "nullcontext",
    "version",
]
