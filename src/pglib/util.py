import os
import tempfile
from pathlib import Path

from . import cmd
from .types import CommandRunner


def xdg_data_home() -> Path:
    return Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))


def generate_certificate(
    configdir: Path, *, run_command: CommandRunner = cmd.run
) -> None:
    """Generate a self-signed certificate for a PostgreSQL instance in
    `configdir`.
    """
    certfile = configdir / "server.crt"
    keyfile = configdir / "server.key"
    run_command(["openssl", "genrsa", "-out", str(keyfile), "2048"], check=True)
    keyfile.chmod(0o600)
    out = run_command(
        ["openssl", "req", "-new", "-text", "-key", str(keyfile), "-batch"],
        check=True,
    ).stdout
    with tempfile.NamedTemporaryFile("w") as tempcert:
        tempcert.write(out)
        tempcert.seek(0)
        run_command(
            [
                "openssl",
                "req",
                "-x509",
                "-text",
                "-in",
                tempcert.name,
                "-key",
                str(keyfile),
                "-out",
                str(certfile),
            ],
            check=True,
        )
    certfile.chmod(0o600)


def short_version(version: int) -> str:
    """Convert a server version as per PQServerVersion to a major version string

    >>> short_version(90603)
    '9.6'
    >>> short_version(100001)
    '10'
    >>> short_version(110011)
    '11'
    """
    ret = version / 10000
    if ret < 10:
        ret = int(ret) + int(version % 1000 / 100) / 10
    else:
        ret = int(ret)
    return str(ret)
