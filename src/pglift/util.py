import os
import tempfile
from pathlib import Path

import humanize

from . import cmd, exceptions
from .types import CommandRunner


def xdg_data_home() -> Path:
    return Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))


def xdg_runtime_dir() -> Path:
    return Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"))


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


def parse_filesize(value: str) -> float:
    """Parse a file size string as float, in bytes unit.

    >>> parse_filesize("6022056 kB")
    6166585344.0
    >>> parse_filesize("0")
    Traceback (most recent call last):
        ...
    ValueError: malformatted file size '0'
    >>> parse_filesize("5 km")
    Traceback (most recent call last):
        ...
    ValueError: invalid unit 'km'
    >>> parse_filesize("5 yb")
    Traceback (most recent call last):
        ...
    ValueError: invalid unit 'yb'
    """
    units = ["B", "K", "M", "G", "T"]
    try:
        val, unit = value.split(None, 1)
        mult, b = list(unit)
    except ValueError:
        raise ValueError(f"malformatted file size '{value}'") from None
    if b.lower() != "b":
        raise ValueError(f"invalid unit '{unit}'")
    try:
        scale = units.index(mult.upper())
    except ValueError:
        raise ValueError(f"invalid unit '{unit}'") from None
    assert isinstance(scale, int)
    return (1024 ** scale) * float(val)  # type: ignore[no-any-return]


def total_memory(path: Path = Path("/proc/meminfo")) -> float:
    """Read 'MemTotal' field from /proc/meminfo.

    :raise ~exceptions.SystemError: if reading the value failed.
    """
    with path.open() as meminfo:
        for line in meminfo:
            if not line.startswith("MemTotal:"):
                continue
            return parse_filesize(line.split(":", 1)[-1].strip())
        else:
            raise exceptions.SystemError(
                f"could not retrieve memory information from {path}"
            )


def percent_memory(value: str, total: float) -> str:
    """Convert 'value' from a percentage of total memory into a memory setting
    or return (as is if not a percentage value).

    >>> percent_memory(" 1GB", 1)
    '1GB'
    >>> percent_memory("25%", 4e9)
    '1 GB'
    >>> percent_memory("xyz%", 3e9)
    Traceback (most recent call last):
      ...
    ValueError: invalid percent value 'xyz'
    """
    value = value.strip()
    if value.endswith("%"):
        value = value[:-1].strip()
        try:
            percent_value = float(value) / 100
        except ValueError:
            raise ValueError(f"invalid percent value '{value}'")
        value = humanize.naturalsize(total * percent_value, format="%d")
    return value
