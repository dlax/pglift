import configparser
import datetime
import enum
import json
import logging
import os
import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union, overload

from dateutil.tz import gettz
from pgtoolkit import conf as pgconf
from typing_extensions import Literal

from . import exceptions, hookimpl
from . import instance as instance_mod
from . import roles, util
from .conf import info as conf_info
from .models import interface, system
from .models.interface import InstanceBackup
from .settings import PgBackRestSettings
from .task import task
from .types import AutoStrEnum

if TYPE_CHECKING:
    from .ctx import BaseContext

logger = logging.getLogger(__name__)


def available(ctx: "BaseContext") -> Optional[PgBackRestSettings]:
    return ctx.settings.pgbackrest


def make_cmd(
    instance: system.BaseInstance, settings: PgBackRestSettings, *args: str
) -> List[str]:
    configpath = _configpath(instance, settings)
    stanza = _stanza(instance)
    return [
        str(settings.execpath),
        f"--config={configpath}",
        f"--stanza={stanza}",
    ] + list(args)


def _configpath(instance: system.BaseInstance, settings: PgBackRestSettings) -> Path:
    return Path(str(settings.configpath).format(instance=instance))


def _stanza(instance: system.BaseInstance) -> str:
    return f"{instance.version}-{instance.name}"


@overload
def backup_info(
    ctx: "BaseContext",
    instance: system.BaseInstance,
    settings: PgBackRestSettings,
    *,
    backup_set: Optional[str] = None,
    output_json: Literal[False],
) -> str:
    ...


@overload
def backup_info(
    ctx: "BaseContext",
    instance: system.BaseInstance,
    settings: PgBackRestSettings,
    *,
    backup_set: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ...


def backup_info(
    ctx: "BaseContext",
    instance: system.BaseInstance,
    settings: PgBackRestSettings,
    *,
    backup_set: Optional[str] = None,
    output_json: bool = True,
) -> Union[List[Dict[str, Any]], str]:
    """Call pgbackrest info command to obtain information about backups.

    Ref.: https://pgbackrest.org/command.html#command-info
    """
    args = []
    if backup_set is not None:
        args.append(f"--set={backup_set}")
    if output_json:
        args.append("--output=json")
    args.append("info")
    r = ctx.run(make_cmd(instance, settings, *args), check=True)
    if not output_json:
        return r.stdout
    return json.loads(r.stdout)  # type: ignore[no-any-return]


@task("setting up pgBackRest")
def setup(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    settings: PgBackRestSettings,
) -> None:
    """Setup pgBackRest"""
    configpath = _configpath(instance, settings)
    configpath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)
    directory = Path(str(settings.directory).format(instance=instance))
    logpath = Path(str(settings.logpath).format(instance=instance))
    logpath.mkdir(exist_ok=True, parents=True)
    spoolpath = Path(str(settings.spoolpath).format(instance=instance))
    spoolpath.mkdir(exist_ok=True, parents=True)
    lockpath = Path(str(settings.lockpath).format(instance=instance))
    lockpath.mkdir(exist_ok=True, parents=True)

    instance_config = instance.config()
    stanza = _stanza(instance)

    # Always use string values so that this would match with actual config (on
    # disk) that's parsed later on.
    config = {
        "global": {
            "repo1-path": str(directory),
            "repo1-retention-archive": "2",
            "repo1-retention-diff": "3",
            "repo1-retention-full": "2",
            "lock-path": str(lockpath),
            "log-path": str(logpath),
            "spool-path": str(spoolpath),
        },
        "global:archive-push": {
            "compress-level": "3",
        },
        stanza: {
            "pg1-path": f"{instance.datadir}",
            "pg1-port": str(instance.port),
            "pg1-user": ctx.settings.postgresql.backuprole,
        },
    }
    unix_socket_directories = instance_config.get("unix_socket_directories")
    if unix_socket_directories:
        config[stanza]["pg1-socket-path"] = str(instance_config.unix_socket_directories)
    cp = configparser.ConfigParser()
    actual_config = {}
    if configpath.exists():
        cp.read(configpath)
        actual_config = {name: dict(cp.items(name)) for name in config}
    if config != actual_config:
        cp.read_dict(config)

        with configpath.open("w") as configfile:
            cp.write(configfile)

    directory.mkdir(exist_ok=True, parents=True)

    base_cmd = make_cmd(instance, settings)

    configdir = instance.datadir
    confd = conf_info(configdir)[0]
    pgconfigfile = confd / "pgbackrest.conf"
    if not pgconfigfile.exists():
        pgconfig = pgconf.Configuration()
        pgconfig.archive_command = " ".join(base_cmd + ["archive-push", "%p"])
        pgconfig.archive_mode = "on"
        pgconfig.max_wal_senders = 3
        pgconfig.wal_level = "replica"

        with pgconfigfile.open("w") as f:
            pgconfig.save(f)


@setup.revert("deconfiguring pgBackRest")
def revert_setup(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    settings: PgBackRestSettings,
) -> None:
    """Un-setup pgBackRest"""
    configpath = _configpath(instance, settings)
    directory = Path(str(settings.directory).format(instance=instance))

    if configpath.exists():
        configpath.unlink()

    try:
        shutil.rmtree(directory)
    except FileNotFoundError:
        pass

    configdir = instance.datadir
    confd = conf_info(configdir)[0]
    pgconfigfile = confd / "pgbackrest.conf"
    if pgconfigfile.exists():
        pgconfigfile.unlink()


@task("initializing pgBackRest repository")
def init(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    settings: PgBackRestSettings,
) -> None:
    with instance_mod.running(ctx, instance):
        role = interface.Role(
            name=ctx.settings.postgresql.backuprole,
            password=util.generate_password(),
            login=True,
            superuser=True,
            pgpass=True,
        )
        if not roles.exists(ctx, instance, role.name):
            roles.create(ctx, instance, role)
            roles.set_pgpass_entry_for(ctx, instance, role)
        ctx.run(make_cmd(instance, settings, "start"), check=True)
        ctx.run(make_cmd(instance, settings, "stanza-create"), check=True)
        ctx.run(make_cmd(instance, settings, "check"), check=True)


@hookimpl  # type: ignore[misc]
def instance_configure(ctx: "BaseContext", manifest: interface.Instance) -> None:
    """Install pgBackRest for an instance when it gets configured."""
    settings = available(ctx)
    if not settings:
        logger.warning("pgbackrest not available, skipping backup configuration")
        return
    instance = system.Instance.system_lookup(ctx, (manifest.name, manifest.version))
    if instance.standby:
        return
    setup(ctx, instance, settings)

    info_json = backup_info(ctx, instance, settings)
    # Only initialize if the stanza does not already exists.
    if not info_json or info_json[0]["status"]["code"] == 1:
        init(ctx, instance, settings)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: "BaseContext", instance: system.Instance) -> None:
    """Uninstall pgBackRest from an instance being dropped."""
    settings = available(ctx)
    if not settings:
        return
    if instance.standby:
        return
    revert_setup(ctx, instance, settings)


class BackupType(AutoStrEnum):
    """Backup type."""

    full = enum.auto()
    """full backup"""
    incr = enum.auto()
    """incremental backup"""
    diff = enum.auto()
    """differential backup"""

    @classmethod
    def default(cls) -> "BackupType":
        return cls.incr


def backup_command(
    instance: system.BaseInstance,
    settings: PgBackRestSettings,
    *,
    type: BackupType = BackupType.default(),
    start_fast: bool = True,
) -> List[str]:
    """Return the full pgbackrest command to perform a backup for ``instance``.

    :param type: backup type (one of 'full', 'incr', 'diff').

    Ref.: https://pgbackrest.org/command.html#command-backup
    """
    args = [
        f"--type={type.name}",
        "--log-level-console=info",
        "backup",
    ]
    if start_fast:
        args.insert(-1, "--start-fast")
    return make_cmd(instance, settings, *args)


@task("backing up instance with pgBackRest")
def backup(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    settings: PgBackRestSettings,
    *,
    type: BackupType = BackupType.default(),
) -> None:
    """Perform a backup of ``instance``.

    :param type: backup type (one of 'full', 'incr', 'diff').

    Ref.: https://pgbackrest.org/command.html#command-backup
    """
    if instance.standby:
        raise exceptions.InstanceStateError("backup should be done on primary instance")

    # Don't use ctx.libpq_environ() here since it applies to surole and we use
    # backuprole.
    env = os.environ.copy()
    env["PGPASSFILE"] = str(ctx.settings.postgresql.auth.passfile)
    ctx.run(
        backup_command(instance, settings, type=type),
        check=True,
        env=env,
    )


def expire_command(
    instance: system.BaseInstance, settings: PgBackRestSettings
) -> List[str]:
    """Return the full pgbackrest command to expire backups for ``instance``.

    Ref.: https://pgbackrest.org/command.html#command-expire
    """
    return make_cmd(instance, settings, "--log-level-console=info", "expire")


@task("expiring pgBackRest backups")
def expire(
    ctx: "BaseContext", instance: system.BaseInstance, settings: PgBackRestSettings
) -> None:
    """Expire a backup of ``instance``.

    Ref.: https://pgbackrest.org/command.html#command-expire
    """
    ctx.run(expire_command(instance, settings), check=True)


def _parse_backup_databases(info: str) -> List[str]:
    """Parse output of pgbackrest info --set=<label> and return the list of
    databases.

    This is only required until "pgbackrest info" accepts options --set and
    --output=json together.

    >>> set_info = '''stanza: 13-main
    ... status: ok
    ... cipher: none
    ...
    ... db (current)
    ...     wal archive min/max (13-1): 000000010000000000000001/000000010000000000000004
    ...
    ...     full backup: 20210121-153336F
    ...         timestamp start/stop: 2021-01-21 15:33:36 / 2021-01-21 15:33:59
    ...         wal start/stop: 000000010000000000000004 / 000000010000000000000004
    ...         database size: 39.6MB, backup size: 39.6MB
    ...         repository size: 4.9MB, repository backup size: 4.9MB
    ...         database list: bar (16434), foo (16401), postgres (14174)
    ...         symlinks:
    ...             pg_wal => /var/lib/pgsql/13/main/pg_wal_mnt/pg_wal
    ... '''
    >>> _parse_backup_databases(set_info)
    ['bar', 'foo', 'postgres']
    """
    dbs_pattern = re.compile(r"^\s*database list:\s*(.*)$")
    db_pattern = re.compile(r"(\S+)\s*\(.*")
    for line in info.splitlines():
        m = dbs_pattern.match(line)
        if m:
            return [
                re.sub(db_pattern, r"\g<1>", db.strip()) for db in m.group(1).split(",")
            ]
    return []


def iter_backups(
    ctx: "BaseContext", instance: system.BaseInstance, settings: PgBackRestSettings
) -> Iterator[InstanceBackup]:
    """Yield information about backups on an instance."""
    backups = backup_info(ctx, instance, settings)[0]["backup"]

    def started_at(entry: Any) -> float:
        return entry["timestamp"]["start"]  # type: ignore[no-any-return]

    for backup in sorted(backups, key=started_at, reverse=True):
        info_set = backup_info(
            ctx, instance, settings, backup_set=backup["label"], output_json=False
        )
        databases = _parse_backup_databases(info_set)
        dt = datetime.datetime.fromtimestamp(backup["timestamp"]["start"])
        yield InstanceBackup(
            label=backup["label"],
            size=backup["info"]["size"],
            repo_size=backup["info"]["repository"]["size"],
            datetime=dt.replace(tzinfo=gettz()),
            type=backup["type"],
            databases=", ".join(databases),
        )


def restore_command(
    instance: system.BaseInstance,
    settings: PgBackRestSettings,
    *,
    date: Optional[datetime.datetime] = None,
    backup_set: Optional[str] = None,
) -> List[str]:
    """Return the pgbackrest restore for ``instance``.

    Ref.: https://pgbackrest.org/command.html#command-restore
    """
    args = [
        "--log-level-console=info",
        # The delta option allows pgBackRest to handle instance data/wal
        # directories itself, without the need to clean them up beforehand.
        "--delta",
        "--link-all",
        "--target-action=promote",
    ]
    if date is not None:
        target = date.strftime("%Y-%m-%d %H:%M:%S.%f%z")
        args.extend(["--type=time", f"--target={target}"])
    if backup_set is not None:
        args.append(f"--set={backup_set}")
    args.append("restore")
    return make_cmd(instance, settings, *args)


@task("restoring instance with pgBackRest")
def restore(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    settings: PgBackRestSettings,
    *,
    label: Optional[str] = None,
    date: Optional[datetime.datetime] = None,
) -> None:
    """Restore an instance, possibly only including specified databases.

    The instance must not be running.

    Ref.: https://pgbackrest.org/command.html#command-restore
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)

    cmd = restore_command(instance, settings, date=date, backup_set=label)
    ctx.run(cmd, check=True)
