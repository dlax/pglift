import argparse
from typing import Optional, Sequence

from .cmd import execute_program
from .ctx import Context
from .exceptions import InstanceNotFound
from .models.system import PostgreSQLInstance
from .pm import PluginManager

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
        ctx = Context(plugin_manager=PluginManager.get())

    try:
        instance = PostgreSQLInstance.from_stanza(ctx, args.instance)
    except ValueError as e:
        parser.error(str(e))
    except InstanceNotFound as e:
        parser.exit(2, str(e))

    bindir = ctx.settings.postgresql.versions[instance.version].bindir
    cmd = [str(bindir / "postgres"), "-D", str(instance.datadir)]
    pidfile = (
        ctx.settings.postgresql.pid_directory
        / f"postgresql-{instance.version}-{instance.name}.pid"
    )
    execute_program(cmd, pidfile, logger=ctx)


if __name__ == "__main__":  # pragma: nocover
    main()
