import argparse
import subprocess
import sys
from typing import Optional, Sequence

from .ctx import Context
from .model import Instance
from .pm import PluginManager
from .settings import SETTINGS

parser = argparse.ArgumentParser(description="Start postgres for specified instance")
parser.add_argument(
    "instance",
    help="instance identifier as <version>/<name>",
)


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    ctx: Optional[Context] = None,
) -> None:
    args = parser.parse_args(argv)
    try:
        version, name = args.instance.split("/", 1)
        if not name:
            raise ValueError("empty name")
    except ValueError:
        parser.error("invalid instance identifier")

    if ctx is None:
        ctx = Context(plugin_manager=PluginManager.get(), settings=SETTINGS)

    instance = Instance(name, version, settings=ctx.settings)
    if not instance.exists():
        parser.error(f"instance {instance} not found")

    bindir = ctx.settings.postgresql.versions[instance.version].bindir
    cmd = [str(bindir / "postgres"), "-D", str(instance.datadir)]
    piddir = ctx.settings.postgresql.pid_directory
    if not piddir.exists():
        piddir.mkdir(parents=True)
    pidfile = piddir / f"postgresql-{version}-{name}.pid"
    if pidfile.exists():
        sys.exit(f"PID file {pidfile} already exists")
    pid = subprocess.Popen(cmd).pid
    pidfile.write_text(str(pid))


if __name__ == "__main__":  # pragma: nocover
    main()
