import subprocess
from typing import Any, Sequence

from .types import CompletedProcess


def run(args: Sequence[str], **kwargs: Any) -> CompletedProcess:
    return subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        **kwargs,
    )
