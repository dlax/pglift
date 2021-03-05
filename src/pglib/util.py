import tempfile
from pathlib import Path

from . import cmd


def generate_certificate(
    configdir: Path, *, run_command: cmd.CommandRunner = cmd.run
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
