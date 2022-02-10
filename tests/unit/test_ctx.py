from pathlib import Path

from pglift.ctx import Context
from pglift.settings import Settings


def test_libpq_environ(ctx: Context) -> None:
    assert ctx.libpq_environ(base={}) == {
        "PGPASSFILE": str(ctx.settings.postgresql.auth.passfile)
    }
    assert ctx.libpq_environ(base={"PGPASSFILE": "/var/lib/pgsql/pgpass"}) == {
        "PGPASSFILE": "/var/lib/pgsql/pgpass"
    }


def test_libpq_environ_password_command(tmp_path: Path, ctx: Context) -> None:
    passcmd = tmp_path / "passcmd"
    with passcmd.open("w") as f:
        f.write("#!/bin/sh\necho foo")
    passcmd.chmod(0o755)
    ctx = Context(
        settings=Settings.parse_obj(
            {
                "prefix": ctx.settings.prefix,
                "postgresql": {
                    "auth": {
                        "password_command": str(passcmd),
                        "passfile": str(tmp_path / "pgpass"),
                    }
                },
            }
        ),
    )
    assert ctx.libpq_environ(base={}) == {
        "PGPASSFILE": str(tmp_path / "pgpass"),
        "PGPASSWORD": "foo",
    }
