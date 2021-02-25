import subprocess
from pathlib import Path
from typing import Any, Optional

from . import pg
from ._cmd import command
from .task import task


@task
def init(
    *,
    datadir: Path,
    waldir: Path,
    surole: str,
    locale: str,
    pgroot: Path,
    data_checksums: bool = False,
    sysuser: Optional[str] = None,
) -> None:
    """Initialize a PostgreSQL instance."""

    options = [
        f"--pgdata={datadir}",
        "-U",
        surole,
        "-X",
        str(waldir),
        "--encoding=UTF8",
        f"--locale={locale}",
    ]
    if data_checksums:
        options.append("--data-checksums")

    cmd = command(str(pg.binpath("initdb")), user=sysuser)

    subprocess.check_call(cmd + options, cwd=pgroot)


@init.revert
def uninit(
    *,
    datadir: Path,
    waldir: Path,
    sysuser: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Un-initialize a PostgreSQL instance."""
    cmd = command("rm", "-rf", user=sysuser)
    subprocess.check_call(cmd + [str(waldir)])
    subprocess.check_call(cmd + [str(datadir)])
