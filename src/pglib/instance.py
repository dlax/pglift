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

    pgroot.mkdir(mode=0o750, exist_ok=True)

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
def revert_init(
    *,
    datadir: Path,
    waldir: Path,
    pgroot: Path,
    sysuser: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Un-initialize a PostgreSQL instance."""
    cmd = command("rm", "-rf", user=sysuser)
    subprocess.check_call(cmd + [str(waldir)])
    subprocess.check_call(cmd + [str(datadir)])
    try:
        next(pgroot.iterdir())
    except StopIteration:
        # directory is empty
        pgroot.rmdir()


@task
def configure(
    instance: str,
    *,
    configdir: Path,
    filename: str,
    **confitems: Any,
) -> None:
    """Write instance's configuration to 'filename' and include it in its
    postgresql.conf.
    """
    postgresql_conf = configdir / "postgresql.conf"
    assert postgresql_conf.exists()
    original_content = postgresql_conf.read_text()
    with postgresql_conf.open("w") as f:
        f.write(f"include = '{filename}'\n\n")
        f.write(original_content)
    config = pg.make_configuration(instance, **confitems)
    with (configdir / filename).open("w") as f:
        config.save(f)


@configure.revert
def revert_configure(
    instance: str,
    *,
    configdir: Path,
    filename: str,
    **kwargs: Any,
) -> None:
    """Remove custom instance configuration, leaving the default
    'postgresql.conf'.
    """
    filepath = configdir / filename
    if filepath.exists():
        filepath.unlink()
    postgresql_conf = configdir / "postgresql.conf"
    with postgresql_conf.open() as f:
        line = f.readline()
        if line.startswith(f"include = '{filename}'"):
            while line:
                # Move to next non-empty line in file.
                pos = f.tell()
                line = f.readline()
                if line.strip():
                    f.seek(pos)
                    break
            rest = f.read()
            with postgresql_conf.open("w") as nf:
                nf.write(rest)
