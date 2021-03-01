import subprocess
from pathlib import Path
from typing import Any, Tuple, Union

from . import pg, util
from .model import Instance
from .settings import SETTINGS, PostgreSQLSettings
from .task import task

POSTGRESQL_SETTINGS = SETTINGS.postgresql


@task
def init(
    instance: Instance,
    *,
    data_checksums: bool = False,
    settings: PostgreSQLSettings = POSTGRESQL_SETTINGS,
) -> None:
    """Initialize a PostgreSQL instance."""

    pgroot = settings.root
    pgroot.mkdir(mode=0o750, exist_ok=True)

    cmd = [
        str(pg.binpath("initdb")),
        f"--pgdata={instance.datadir}",
        f"--waldir={instance.waldir}",
        f"--username={settings.surole}",
        "--encoding=UTF8",
    ]
    if settings.locale:
        cmd.append(f"--locale={settings.locale}")
    if data_checksums:
        cmd.append("--data-checksums")

    subprocess.check_call(cmd, cwd=pgroot)


@init.revert
def revert_init(
    instance: Instance,
    *,
    settings: PostgreSQLSettings = POSTGRESQL_SETTINGS,
    **kwargs: Any,
) -> None:
    """Un-initialize a PostgreSQL instance."""
    subprocess.check_call(["rm", "-rf", str(instance.waldir)])
    subprocess.check_call(["rm", "-rf", str(instance.datadir)])
    pgroot = settings.root
    try:
        next(pgroot.iterdir())
    except StopIteration:
        # directory is empty
        pgroot.rmdir()


@task
def configure(
    instance: Instance,
    *,
    filename: str,
    ssl: Union[bool, Tuple[Path, Path]] = False,
    settings: PostgreSQLSettings = POSTGRESQL_SETTINGS,
    **confitems: Any,
) -> None:
    """Write instance's configuration to 'filename' and include it in its
    postgresql.conf.

    `ssl` parameter controls SSL configuration. If False, SSL is not enabled.
    If True, a self-signed certificate is generated. A tuple of two
    `~pathlib.Path` corresponding to the location of SSL cert file and key
    file to use may also be passed.
    """
    configdir = instance.datadir
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
    confitems.setdefault("port", instance.port)
    config = pg.make_configuration(instance.name, **confitems)
    with (configdir / filename).open("w") as f:
        config.save(f)


@configure.revert
def revert_configure(
    instance: Instance,
    *,
    filename: str,
    ssl: Union[bool, Tuple[Path, Path]] = False,
    **kwargs: Any,
) -> None:
    """Remove custom instance configuration, leaving the default
    'postgresql.conf'.
    """
    configdir = instance.datadir
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
