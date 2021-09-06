import shlex
from typing import Iterable

try:
    shlex_join = shlex.join  # type: ignore[attr-defined]
except AttributeError:

    def shlex_join(split_command: Iterable[str]) -> str:
        return " ".join(shlex.quote(arg) for arg in split_command)
