import subprocess
from pathlib import Path
from typing import Any, Optional, Tuple, Union

from . import pg, util
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
) -> None:
    """Initialize a PostgreSQL instance."""

    pgroot.mkdir(mode=0o750, exist_ok=True)

    cmd = [
        str(pg.binpath("initdb")),
        f"--pgdata={datadir}",
        "-U",
        surole,
        "-X",
        str(waldir),
        "--encoding=UTF8",
        f"--locale={locale}",
    ]
    if data_checksums:
        cmd.append("--data-checksums")

    subprocess.check_call(cmd, cwd=pgroot)


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
    subprocess.check_call(["rm", "-rf", str(waldir)])
    subprocess.check_call(["rm", "-rf", str(datadir)])
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
    ssl: Union[bool, Tuple[Path, Path]] = False,
    **confitems: Any,
) -> None:
    """Write instance's configuration to 'filename' and include it in its
    postgresql.conf.

    `ssl` parameter controls SSL configuration. If False, SSL is not enabled.
    If True, a self-signed certificate is generated. A tuple of two
    `~pathlib.Path` corresponding to the location of SSL cert file and key
    file to use may also be passed.
    """
    postgresql_conf = configdir / "postgresql.conf"
    assert postgresql_conf.exists()
    if ssl is True:
        util.generate_certificate(configdir)
        confitems["ssl"] = True
    elif isinstance(ssl, tuple):
        try:
            certfile, keyfile = ssl
        except ValueError:
            raise ValueError("expecting a 2-tuple for 'ssl' parameter") from None
        confitems["ssl"] = True
        confitems["ssl_cert_file"] = certfile
        confitems["ssl_key_file"] = keyfile
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
    ssl: Union[bool, Tuple[Path, Path]] = False,
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
    if ssl is True:
        for ext in ("crt", "key"):
            fpath = configdir / f"server.{ext}"
            if fpath.exists():
                fpath.unlink()
