from pathlib import Path
from typing import Optional

from pgtoolkit.pgpass import PassEntry
from pgtoolkit.pgpass import parse as pgpass_parse


def add(
    passfile: Path,
    password: str,
    *,
    hostname: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
) -> None:
    """Add an entry to user's .pgpass."""
    passfile.touch(mode=0o600)
    pgpass = pgpass_parse(str(passfile))

    match_rule = {
        "hostname": hostname or "*",
        "port": str(port or "*"),
        "database": database or "*",
        "username": username or "*",
    }
    for entry in pgpass:
        if entry.matches(**match_rule):
            entry.password = password
            break
    else:
        pgpass.lines.append(PassEntry(password=password, **match_rule))
        pgpass.sort()

    pgpass.save()


def remove(
    passfile: Path,
    *,
    port: int,
    database: Optional[str] = None,
    username: Optional[str] = None,
) -> None:
    """Remove an entry user's .pgpass."""
    if not passfile.exists():
        return
    pgpass = pgpass_parse(str(passfile))
    match_rule = {}
    if database is not None:
        match_rule["database"] = database
    if username is not None:
        match_rule["username"] = username
    pgpass.remove(filter=None, port=port, **match_rule)
    if pgpass.lines:
        pgpass.save()
    else:
        passfile.unlink()
