from .ctx import BaseContext, Context
from .model import Instance
from .pgbackrest import BackupType, backup, expire

if __name__ == "__main__":  # pragma: nocover
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--version")
    parser.add_argument("--instance", metavar="NAME", required=True)
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
    ctx = Context()
    if args.version:
        instance = Instance(args.instance, args.version)
    else:
        instance = Instance.default_version(args.instance, ctx=ctx)
    args.func(ctx, instance, args)
