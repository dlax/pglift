import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pytest

from pglift import instance as instance_mod
from pglift import pgbackrest
from pglift.conf import info as conf_info
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.settings import PgBackRestSettings

from . import execute, reconfigure_instance
from .conftest import DatabaseFactory

pytestmark = pytest.mark.skipif(
    shutil.which("pgbackrest") is None, reason="pgbackrest is not available"
)


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
    pgbackrest.setup(ctx, instance, pgbackrest_settings)
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

    before = datetime.now()
    assert not latest_backup.exists()
    with instance_mod.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT datname FROM pg_database")
        assert "backrest" in [r["datname"] for r in rows]
        pgbackrest.backup(
            ctx,
            instance,
            pgbackrest_settings,
            type=pgbackrest.BackupType.full,
        )
        assert latest_backup.exists() and latest_backup.is_symlink()
        pgbackrest.expire(ctx, instance, pgbackrest_settings)
        # TODO: check some result from 'expire' command here.

        time.sleep(1)
        (record,) = execute(ctx, instance, "SELECT current_timestamp", fetch=True)
        before_drop = record["current_timestamp"]

        execute(ctx, instance, "DROP DATABASE backrest", autocommit=True, fetch=False)

    (backup1,) = list(pgbackrest.iter_backups(ctx, instance, pgbackrest_settings))
    assert backup1.type == "full"
    assert backup1.databases == "backrest, postgres"
    assert backup1.datetime.replace(tzinfo=None) > before

    pgbackrest.restore(ctx, instance, pgbackrest_settings, date=before_drop)
    with instance_mod.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT datname FROM pg_database")
        assert "backrest" in [r["datname"] for r in rows]
