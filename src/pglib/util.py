import subprocess
import tempfile
from pathlib import Path


def generate_certificate(configdir: Path) -> None:
    """Generate a self-signed certificate for a PostgreSQL instance in
    `configdir`.
    """
    certfile = configdir / "server.crt"
    keyfile = configdir / "server.key"
    subprocess.check_call(["openssl", "genrsa", "-out", keyfile, "2048"])
    keyfile.chmod(0o600)
    out = subprocess.check_output(
        ["openssl", "req", "-new", "-text", "-key", keyfile, "-batch"],
    )
    with tempfile.NamedTemporaryFile() as tempcert:
        tempcert.write(out)
        tempcert.seek(0)
        subprocess.check_call(
            [
                "openssl",
                "req",
                "-x509",
                "-text",
                "-in",
                tempcert.name,
                "-key",
                keyfile,
                "-out",
                certfile,
            ]
        )
    certfile.chmod(0o600)
