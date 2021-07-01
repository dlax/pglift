import argparse
import subprocess
import sys
from typing import Optional, Sequence

from .ctx import Context
from .exceptions import InstanceNotFound
from .model import Instance
from .pm import PluginManager
from .settings import SETTINGS

parser = argparse.ArgumentParser(description="Start postgres for specified instance")
parser.add_argument(
    "instance",
    help="instance identifier as <version>-<name>",
)


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    ctx: Optional[Context] = None,
) -> None:
    args = parser.parse_args(argv)
    if ctx is None:
        ctx = Context(plugin_manager=PluginManager.get(), settings=SETTINGS)

    try:
        instance = Instance.from_stanza(args.instance, settings=ctx.settings)
    except ValueError as e:
        parser.error(str(e))
    except InstanceNotFound as e:
        parser.exit(2, e.show())

    bindir = ctx.settings.postgresql.versions[instance.version].bindir
    cmd = [str(bindir / "postgres"), "-D", str(instance.datadir)]
    piddir = ctx.settings.postgresql.pid_directory
    if not piddir.exists():
        piddir.mkdir(parents=True)
    pidfile = piddir / f"postgresql-{instance.version}-{instance.name}.pid"
    if pidfile.exists():
        sys.exit(f"PID file {pidfile} already exists")
    pid = subprocess.Popen(cmd).pid
    pidfile.write_text(str(pid))


if __name__ == "__main__":  # pragma: nocover
    main()
