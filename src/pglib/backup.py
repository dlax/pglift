from .ctx import BaseContext, Context
from .model import Instance
from .pgbackrest import BackupType, backup, expire
from .pm import PluginManager

if __name__ == "__main__":  # pragma: nocover
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "stanza", metavar="<version>-<name>", help="instance identifier"
    )
    subparsers = parser.add_subparsers()

    def do_backup(
        ctx: BaseContext, instance: Instance, args: argparse.Namespace
    ) -> None:
        return backup(ctx, instance, type=BackupType(args.type))

    backup_parser = subparsers.add_parser("backup")
    backup_parser.set_defaults(func=do_backup)
    backup_parser.add_argument(
        "--type",
        choices=[t.name for t in BackupType],
        default=BackupType.default().name,
    )

    def do_expire(
        ctx: BaseContext, instance: Instance, args: argparse.Namespace
    ) -> None:
        return expire(ctx, instance)

    subparsers.add_parser("expire").set_defaults(func=do_expire)

    args = parser.parse_args()
    ctx = Context(plugin_manager=PluginManager.get())
    try:
        instance = Instance.from_stanza(args.stanza)
    except ValueError as e:
        parser.error(str(e))
    args.func(ctx, instance, args)
