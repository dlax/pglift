import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pytest

from pglift import instances, types
from pglift.conf import info as conf_info
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.pgbackrest import impl as pgbackrest
from pglift.settings import PgBackRestSettings

from . import execute, reconfigure_instance
from .conftest import DatabaseFactory


@pytest.fixture(scope="session", autouse=True)
def pgbackrest_available(pgbackrest_available: bool) -> bool:
    if not pgbackrest_available:
        pytest.skip("pgbackrest is not available")
    return pgbackrest_available


@pytest.fixture
def directory(
    pgbackrest_settings: PgBackRestSettings, instance: system.Instance
) -> Path:
    return Path(str(pgbackrest_settings.directory).format(instance=instance))


def test_configure(
    ctx: Context,
    pgbackrest_settings: PgBackRestSettings,
    instance: system.Instance,
    instance_manifest: interface.Instance,
    tmp_path: Path,
    tmp_port_factory: Iterator[int],
    directory: Path,
) -> None:
    instance_config = instance.config()
    assert instance_config
    instance_port = instance_config.port

    configpath = Path(str(pgbackrest_settings.configpath).format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    assert f"pg1-port = {instance_port}" in lines
    assert "pg1-user = backup" in lines
    assert directory.exists()

    lockpath = Path(str(pgbackrest_settings.lockpath).format(instance=instance))
    spoolpath = Path(str(pgbackrest_settings.spoolpath).format(instance=instance))
    assert lockpath.exists()
    assert spoolpath.exists()

    lines = ctx.settings.postgresql.auth.passfile.read_text().splitlines()
    assert any(line.startswith(f"*:{instance.port}:*:backup:") for line in lines)

    configdir = instance.datadir
    confd = conf_info(configdir)[0]
    pgconfigfile = confd / "pgbackrest.conf"
    pgconfig = pgconfigfile.read_text().splitlines()
    assert (
        f"archive_command = '{pgbackrest_settings.execpath} --config={configpath}"
        f" --stanza={instance.version}-{instance.name} archive-push %p'"
    ) in pgconfig

    # Calling setup an other time doesn't overwrite configuration
    mtime_before = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    pgbackrest.setup(ctx, instance, pgbackrest_settings, instance.config())
    mtime_after = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    assert mtime_before == mtime_after

    # If instance's configuration changes, pgbackrest configuration is
    # updated.
    config_before = configpath.read_text()
    new_port = next(tmp_port_factory)
    with reconfigure_instance(ctx, instance_manifest, port=new_port):
        config_after = configpath.read_text()
        assert config_after != config_before
        assert f"pg1-port = {new_port}" in config_after.splitlines()


@pytest.mark.usefixtures("surole_password")
def test_backup_restore(
    ctx: Context,
    pgbackrest_settings: PgBackRestSettings,
    instance: system.Instance,
    directory: Path,
    database_factory: DatabaseFactory,
) -> None:
    latest_backup = (
        directory / "backup" / f"{instance.version}-{instance.name}" / "latest"
    )

    assert (
        directory / f"archive/{instance.version}-{instance.name}/archive.info"
    ).exists()
    assert (
        directory / f"backup/{instance.version}-{instance.name}/backup.info"
    ).exists()

    database_factory("backrest")
    execute(
        ctx,
        instance,
        "CREATE TABLE t AS (SELECT 'created' as s)",
        dbname="backrest",
        fetch=False,
    )

    before = datetime.now()
    assert not latest_backup.exists()
    with instances.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT * FROM t", dbname="backrest")
        assert rows == [{"s": "created"}]

        pgbackrest.backup(
            ctx,
            instance,
            pgbackrest_settings,
            type=types.BackupType.full,
        )
        assert latest_backup.exists() and latest_backup.is_symlink()
        pgbackrest.expire(ctx, instance, pgbackrest_settings)
        # TODO: check some result from 'expire' command here.

        execute(
            ctx,
            instance,
            "INSERT INTO t(s) VALUES ('backup1')",
            dbname="backrest",
            fetch=False,
        )

        time.sleep(1)
        (record,) = execute(ctx, instance, "SELECT current_timestamp", fetch=True)
        before_drop = record["current_timestamp"]

        execute(
            ctx,
            instance,
            "INSERT INTO t(s) VALUES ('before-drop')",
            dbname="backrest",
            fetch=False,
        )

        execute(ctx, instance, "DROP DATABASE backrest", autocommit=True, fetch=False)

    (backup1,) = list(pgbackrest.iter_backups(ctx, instance, pgbackrest_settings))
    assert backup1.type == "full"
    assert set(backup1.databases) & {"backrest", "postgres"}
    assert backup1.date_start.replace(tzinfo=None) > before
    assert backup1.date_stop > backup1.date_start

    # With no target (date or label option), restore *and* apply WALs, thus
    # getting back to the same state as before the restore, i.e. 'backrest'
    # database dropped.
    pgbackrest.restore(ctx, instance, pgbackrest_settings)
    with instances.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT datname FROM pg_database")
        assert "backrest" not in [r["datname"] for r in rows]

    # With a date target, WALs are applied until that date.
    pgbackrest.restore(ctx, instance, pgbackrest_settings, date=before_drop)
    with instances.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT datname FROM pg_database")
        assert "backrest" in [r["datname"] for r in rows]
        rows = execute(ctx, instance, "SELECT * FROM t", dbname="backrest")
        assert set(r["s"] for r in rows) == {"created", "backup1"}

    # With a label target, WALs are not replayed, just restore instance state
    # at specified backup.
    pgbackrest.restore(ctx, instance, pgbackrest_settings, label=backup1.label)
    with instances.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT datname FROM pg_database")
        assert "backrest" in [r["datname"] for r in rows]
        rows = execute(ctx, instance, "SELECT * FROM t", dbname="backrest")
        assert rows == [{"s": "created"}]
