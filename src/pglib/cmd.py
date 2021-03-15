import subprocess
import sys
from typing import Any, Sequence

from .types import CompletedProcess

PY37 = sys.version_info[:1] >= (3, 7)


def run(
    args: Sequence[str],
    *,
    capture_output: bool = False,
    check: bool = False,
    **kwargs: Any,
) -> CompletedProcess:
    """Run a command using subprocess.run()."""
    if PY37:
        kwargs["capture_output"] = capture_output
    elif capture_output:
        if "stdout" in kwargs or "stderr" in kwargs:
            raise ValueError(
                "stdout and stderr arguments may not be used with capture_output"
            )
        kwargs["stdout"] = kwargs["stderr"] = subprocess.PIPE

    return subprocess.run(
        args,
        check=check,
        universal_newlines=True,
        **kwargs,
    )
