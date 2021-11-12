from typing import Any

from . import exceptions, hookimpl, systemd
from .ctx import BaseContext, Context
from .models import interface
from .models.system import BaseInstance, Instance, PostgreSQLInstance
from .pgbackrest import BackupType, backup


def systemd_timer(instance: BaseInstance) -> str:
    return f"pglift-backup@{instance.version}-{instance.name}.timer"


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: BaseContext, manifest: interface.Instance, **kwargs: Any
) -> None:
    """Enable scheduled backup job for configured instance."""
    instance = Instance.system_lookup(ctx, (manifest.name, manifest.version))
    if ctx.settings.scheduler == "systemd":
        unit = systemd_timer(instance)
        systemd.enable(ctx, unit)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Disable scheduled backup job when instance is being dropped."""
    if ctx.settings.scheduler == "systemd":
        systemd.disable(ctx, systemd_timer(instance), now=True)


@hookimpl  # type: ignore[misc]
def instance_start(ctx: BaseContext, instance: Instance) -> None:
    """Start schedule backup job at instance startup."""
    if ctx.settings.scheduler == "systemd":
        systemd.start(ctx, systemd_timer(instance))


@hookimpl  # type: ignore[misc]
def instance_stop(ctx: BaseContext, instance: Instance) -> None:
    """Stop schedule backup job when instance is stopping."""
    if ctx.settings.scheduler == "systemd":
        systemd.stop(ctx, systemd_timer(instance))


# This entry point is used by systemd 'postgresql-backup@' service.
def main() -> None:
    import argparse

    from .pm import PluginManager

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "stanza", metavar="<version>-<name>", help="instance identifier"
    )

    def do_backup(
        ctx: BaseContext, instance: Instance, args: argparse.Namespace
    ) -> None:
        return backup(ctx, instance, type=BackupType(args.type))

    parser.set_defaults(func=do_backup)
    parser.add_argument(
        "--type",
        choices=[t.name for t in BackupType],
        default=BackupType.default().name,
    )

    args = parser.parse_args()
    ctx = Context(plugin_manager=PluginManager.get())
    try:
        instance = PostgreSQLInstance.from_stanza(ctx, args.stanza)
    except ValueError as e:
        parser.error(str(e))
    except exceptions.InstanceNotFound as e:
        parser.exit(2, str(e))
    args.func(ctx, instance, args)


if __name__ == "__main__":  # pragma: nocover
    main()
