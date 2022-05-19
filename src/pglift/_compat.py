import shlex
import sys
from typing import Iterable

pyversion = sys.version_info[:2]

if pyversion >= (3, 8):
    from importlib.metadata import version
    from typing import Final, Literal, Protocol, TypedDict

    shlex_join = shlex.join  # type: ignore[attr-defined]
else:
    from importlib_metadata import version  # type: ignore[no-redef]
    from typing_extensions import (  # type: ignore[misc]
        Final,
        Literal,
        Protocol,
        TypedDict,
    )

    def shlex_join(split_command: Iterable[str]) -> str:
        return " ".join(shlex.quote(arg) for arg in split_command)


__all__ = [
    "Final",
    "Literal",
    "Protocol",
    "TypedDict",
    "version",
]
