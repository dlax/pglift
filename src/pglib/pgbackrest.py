import configparser
import enum
import json
import shutil
from pathlib import Path
from typing import List

from pgtoolkit import conf as pgconf

from .conf import info as conf_info
from .ctx import BaseContext
from .model import Instance
from .settings import SETTINGS, PgBackRestSettings
from .task import task

PGBACKREST_SETTINGS = SETTINGS.pgbackrest


def make_cmd(instance: Instance, settings: PgBackRestSettings, *args: str) -> List[str]:
    """Return the base command for pgbackrest as a list of strings.

    >>> from pglib.settings import PgBackRestSettings
    >>> instance = Instance("test", "11")
    >>> settings = PgBackRestSettings(configpath="/tmp/pgbackrest.conf")
    >>> " ".join(make_cmd(instance, settings, 'stanza-upgrade'))
    '/usr/bin/pgbackrest --config=/tmp/pgbackrest.conf --stanza=11-test stanza-upgrade'
    """
    configpath = _configpath(instance, settings)
    stanza = _stanza(instance)
    return [settings.execpath, f"--config={configpath}", f"--stanza={stanza}"] + list(
        args
    )


def _configpath(
    instance: Instance, settings: PgBackRestSettings = PGBACKREST_SETTINGS
) -> Path:
    return Path(settings.configpath.format(instance=instance))


def _stanza(instance: Instance) -> str:
    return f"{instance.version}-{instance.name}"


@task
def setup(
    ctx: BaseContext,
    instance: Instance,
    *,
    settings: PgBackRestSettings = PGBACKREST_SETTINGS,
) -> None:
    """Setup pgBackRest"""
    configpath = _configpath(instance, settings)
    directory = Path(settings.directory.format(instance=instance))
    logpath = Path(settings.logpath.format(instance=instance))
    # Create directory
    configpath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)
    # Create logs directory
    logpath.mkdir(exist_ok=True, parents=True)

    instance_config = instance.config()
    assert instance_config
    stanza = _stanza(instance)

    # Write configuration
    if not configpath.exists():
        config = configparser.ConfigParser()
        config["global"] = {
            "repo1-path": str(directory),
            "log-path": str(logpath),
        }
        config["global:archive-push"] = {
            "compress-level": "3",
        }
        config[stanza] = {
            "pg1-path": f"{instance.datadir}",
            "pg1-port": str(instance_config.port),
            "pg1-user": "postgres",
        }
        if instance_config.unix_socket_directories:
            config[stanza]["pg1-socket-path"] = str(
                instance_config.unix_socket_directories
            )

        with configpath.open("w") as configfile:
            config.write(configfile)

    # Create directories tree for backups
    directory.mkdir(exist_ok=True, parents=True)

    base_cmd = make_cmd(instance, settings)

    # Configure postgres archiving
    configdir = instance.datadir
    pgconfigfile = conf_info(configdir, name="pgbackrest.conf")[1]
    if not pgconfigfile.exists():
        pgconfig = pgconf.Configuration()
        pgconfig.archive_command = " ".join(base_cmd + ["archive-push", "%p"])
        pgconfig.archive_mode = "on"
        pgconfig.listen_addresses = "*"
        pgconfig.log_line_prefix = ""
        pgconfig.max_wal_senders = 3
        pgconfig.wal_level = "replica"

        with pgconfigfile.open("w") as f:
            pgconfig.save(f)


@setup.revert
def revert_setup(
    ctx: BaseContext,
    instance: Instance,
    *,
    settings: PgBackRestSettings = PGBACKREST_SETTINGS,
) -> None:
    """Un-setup pgBackRest"""
    configpath = Path(settings.configpath.format(instance=instance))
    directory = Path(settings.directory.format(instance=instance))

    # Remove configuration file
    if configpath.exists():
        configpath.unlink()

    # Drop directories tree for backups
    try:
        shutil.rmtree(directory)
    except FileNotFoundError:
        pass

    # Remove pg configfile
    configdir = instance.datadir
    pgconfigfile = conf_info(configdir, name="pgbackrest.conf")[1]
    if pgconfigfile.exists():
        pgconfigfile.unlink()


@task
def init(
    ctx: BaseContext,
    instance: Instance,
    *,
    settings: PgBackRestSettings = PGBACKREST_SETTINGS,
) -> None:
    base_cmd = make_cmd(instance, settings)

    info = ctx.run(
        base_cmd + ["--output=json", "info"], check=True, capture_output=True
    ).stdout
    info_json = json.loads(info)
    # If the stanza already exists, don't do anything
    if info_json and info_json[0]["status"]["code"] != 1:
        return
    # Start pgBackRest
    ctx.run(base_cmd + ["start"], check=True)
    # Create the Stanza
    ctx.run(base_cmd + ["stanza-create"], check=True)
    # Check the configuration
    ctx.run(base_cmd + ["check"], check=True)


@enum.unique
class BackupType(enum.Enum):
    """Backup type."""

    full = "full"
    """full backup"""
    incr = "incr"
    """incremental backup"""
    diff = "diff"
    """differential backup"""

    @classmethod
    def default(cls) -> "BackupType":
        return cls.incr


def backup_command(
    instance: Instance,
    *,
    type: BackupType = BackupType.default(),
    settings: PgBackRestSettings = PGBACKREST_SETTINGS,
) -> List[str]:
    """Return the full pgbackrest command to perform a backup for ``instance``.

    :param type: backup type (one of 'full', 'incr', 'diff').

    Ref.: https://pgbackrest.org/command.html#command-backup

    >>> instance = Instance("backmeup", "13")
    >>> print(" ".join(backup_command(instance, type=BackupType.full)))  # doctest: +NORMALIZE_WHITESPACE
    /usr/bin/pgbackrest
        --config=/etc/pgbackrest/pgbackrest-13-backmeup.conf
        --stanza=13-backmeup --type=full
        --repo1-retention-full=9999999
        --repo1-retention-archive=9999999
        --repo1-retention-diff=9999999
        backup
    """
    args = [
        f"--type={type.name}",
        "--repo1-retention-full=9999999",
        "--repo1-retention-archive=9999999",
        "--repo1-retention-diff=9999999",
        "backup",
    ]
    return make_cmd(instance, settings, *args)


@task
def backup(
    ctx: BaseContext,
    instance: Instance,
    *,
    type: BackupType = BackupType.default(),
    settings: PgBackRestSettings = PGBACKREST_SETTINGS,
) -> None:
    """Perform a backup of ``instance``.

    :param type: backup type (one of 'full', 'incr', 'diff').

    Ref.: https://pgbackrest.org/command.html#command-backup
    """
    ctx.run(backup_command(instance, type=type, settings=settings), check=True)


def expire_command(
    instance: Instance,
    *,
    settings: PgBackRestSettings = PGBACKREST_SETTINGS,
) -> List[str]:
    """Return the full pgbackrest command to expire backups for ``instance``.

    Ref.: https://pgbackrest.org/command.html#command-expire

    >>> instance = Instance("backmeup", "13")
    >>> print(" ".join(expire_command(instance)))  # doctest: +NORMALIZE_WHITESPACE
    /usr/bin/pgbackrest
        --config=/etc/pgbackrest/pgbackrest-13-backmeup.conf
        --stanza=13-backmeup
        expire
    """
    return make_cmd(instance, settings, "expire")


@task
def expire(
    ctx: BaseContext,
    instance: Instance,
    *,
    settings: PgBackRestSettings = PGBACKREST_SETTINGS,
) -> None:
    """Expire a backup of ``instance``.

    Ref.: https://pgbackrest.org/command.html#command-expire
    """
    ctx.run(expire_command(instance, settings=settings), check=True)


if __name__ == "__main__":
    import argparse

    from .ctx import Context

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
