import configparser
import enum
import json
import shutil
from pathlib import Path
from typing import Any, List

from pgtoolkit import conf as pgconf

from . import hookimpl
from . import instance as instance_mod
from .conf import info as conf_info
from .ctx import BaseContext
from .model import Instance
from .settings import PgBackRestSettings
from .task import task


def make_cmd(instance: Instance, settings: PgBackRestSettings, *args: str) -> List[str]:
    """Return the base command for pgbackrest as a list of strings.

    >>> from pglift.settings import Settings
    >>> settings = Settings.parse_obj({"pgbackrest": {"configpath": "/tmp/pgbackrest.conf"}})
    >>> instance = Instance("test", "11", settings)
    >>> " ".join(make_cmd(instance, settings.pgbackrest, 'stanza-upgrade'))
    '/usr/bin/pgbackrest --config=/tmp/pgbackrest.conf --stanza=11-test stanza-upgrade'
    """
    configpath = _configpath(instance, settings)
    stanza = _stanza(instance)
    return [
        str(settings.execpath),
        f"--config={configpath}",
        f"--stanza={stanza}",
    ] + list(args)


def _configpath(instance: Instance, settings: PgBackRestSettings) -> Path:
    return Path(str(settings.configpath).format(instance=instance))


def _stanza(instance: Instance) -> str:
    return f"{instance.version}-{instance.name}"


@task
def setup(ctx: BaseContext, instance: Instance) -> None:
    """Setup pgBackRest"""
    settings = ctx.settings.pgbackrest
    configpath = _configpath(instance, settings)
    directory = Path(str(settings.directory).format(instance=instance))
    logpath = Path(str(settings.logpath).format(instance=instance))
    configpath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)
    logpath.mkdir(exist_ok=True, parents=True)

    instance_config = instance.config()
    stanza = _stanza(instance)

    backuprole = ctx.settings.postgresql.surole

    config = {
        "global": {
            "repo1-path": str(directory),
            "log-path": str(logpath),
        },
        "global:archive-push": {
            "compress-level": "3",
        },
        stanza: {
            "pg1-path": f"{instance.datadir}",
            "pg1-port": str(instance.port),
            "pg1-user": backuprole.name,
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
        pgconfig.listen_addresses = "*"
        pgconfig.log_line_prefix = ""
        pgconfig.max_wal_senders = 3
        pgconfig.wal_level = "replica"

        with pgconfigfile.open("w") as f:
            pgconfig.save(f)


@setup.revert
def revert_setup(ctx: BaseContext, instance: Instance) -> None:
    """Un-setup pgBackRest"""
    settings = ctx.settings.pgbackrest
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


@task
def init(ctx: BaseContext, instance: Instance) -> None:
    settings = ctx.settings.pgbackrest
    base_cmd = make_cmd(instance, settings)

    info = ctx.run(base_cmd + ["--output=json", "info"], check=True).stdout
    info_json = json.loads(info)
    # If the stanza already exists, don't do anything
    if info_json and info_json[0]["status"]["code"] != 1:
        return

    with instance_mod.running(ctx, instance):
        ctx.run(base_cmd + ["start"], check=True)
        ctx.run(base_cmd + ["stanza-create"], check=True)
        ctx.run(base_cmd + ["check"], check=True)


@hookimpl  # type: ignore[misc]
def instance_configure(ctx: BaseContext, instance: Instance, **kwargs: Any) -> None:
    """Install pgBackRest for an instance when it gets configured."""
    setup(ctx, instance)
    init(ctx, instance)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Uninstall pgBackRest from an instance being dropped."""
    revert_setup(ctx, instance)


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
    instance: Instance, *, type: BackupType = BackupType.default()
) -> List[str]:
    """Return the full pgbackrest command to perform a backup for ``instance``.

    :param type: backup type (one of 'full', 'incr', 'diff').

    Ref.: https://pgbackrest.org/command.html#command-backup

    >>> from pglift.settings import Settings
    >>> instance = Instance("backmeup", "13", Settings())
    >>> print(" ".join(backup_command(instance, type=BackupType.full)))  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    /usr/bin/pgbackrest
        --config=...etc/pgbackrest/pgbackrest-13-backmeup.conf
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
    return make_cmd(instance, instance.settings.pgbackrest, *args)


@task
def backup(
    ctx: BaseContext,
    instance: Instance,
    *,
    type: BackupType = BackupType.default(),
) -> None:
    """Perform a backup of ``instance``.

    :param type: backup type (one of 'full', 'incr', 'diff').

    Ref.: https://pgbackrest.org/command.html#command-backup
    """
    backuprole = ctx.settings.postgresql.surole
    env = ctx.settings.postgresql.auth.libpq_environ(backuprole)
    ctx.run(backup_command(instance, type=type), check=True, env=env)


def expire_command(instance: Instance) -> List[str]:
    """Return the full pgbackrest command to expire backups for ``instance``.

    Ref.: https://pgbackrest.org/command.html#command-expire

    >>> from pglift.settings import Settings
    >>> instance = Instance("backmeup", "13", Settings())
    >>> print(" ".join(expire_command(instance)))  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    /usr/bin/pgbackrest
        --config=...etc/pgbackrest/pgbackrest-13-backmeup.conf
        --stanza=13-backmeup
        expire
    """
    return make_cmd(instance, instance.settings.pgbackrest, "expire")


@task
def expire(ctx: BaseContext, instance: Instance) -> None:
    """Expire a backup of ``instance``.

    Ref.: https://pgbackrest.org/command.html#command-expire
    """
    ctx.run(expire_command(instance), check=True)
