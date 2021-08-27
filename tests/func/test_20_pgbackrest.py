import shutil
from datetime import datetime
from pathlib import Path

import pytest

from pglift import instance as instance_mod
from pglift import pgbackrest
from pglift.conf import info as conf_info

from . import execute, reconfigure_instance

pytestmark = pytest.mark.skipif(
    shutil.which("pgbackrest") is None, reason="pgbackrest is not available"
)


@pytest.fixture
def directory(ctx, instance):
    pgbackrest_settings = ctx.settings.pgbackrest
    return Path(str(pgbackrest_settings.directory).format(instance=instance))


def test_configure(ctx, installed, instance, tmp_path, tmp_port_factory, directory):
    instance_config = instance.config()
    assert instance_config
    instance_port = instance_config.port
    pgbackrest_settings = ctx.settings.pgbackrest

    configpath = Path(str(pgbackrest_settings.configpath).format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    assert f"pg1-port = {instance_port}" in lines
    assert directory.exists()

    # Calling setup an other time doesn't overwrite configuration
    configdir = instance.datadir
    confd = conf_info(configdir)[0]
    pgconfigfile = confd / "pgbackrest.conf"
    mtime_before = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    pgbackrest.setup(ctx, instance)
    mtime_after = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    assert mtime_before == mtime_after

    # If instance's configuration changes, pgbackrest configuration is
    # updated.
    config_before = configpath.read_text()
    new_port = next(tmp_port_factory)
    with reconfigure_instance(ctx, instance, port=new_port):
        config_after = configpath.read_text()
        assert config_after != config_before
        assert f"pg1-port = {new_port}" in config_after.splitlines()


def test_backup_restore(ctx, instance, directory, database_factory):
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
        assert "backrest" in [r[0] for r in rows]
        pgbackrest.backup(
            ctx,
            instance,
            type=pgbackrest.BackupType.full,
        )
        assert latest_backup.exists() and latest_backup.is_symlink()
        pgbackrest.expire(ctx, instance)
        # TODO: check some result from 'expire' command here.

        ((before_drop,),) = execute(
            ctx, instance, "SELECT current_timestamp", fetch=True
        )

        execute(ctx, instance, "DROP DATABASE backrest", autocommit=True, fetch=False)

    (backup1,) = list(pgbackrest.iter_backups(ctx, instance))
    assert backup1.type == "full"
    assert backup1.databases == "backrest, postgres"
    assert backup1.datetime.replace(tzinfo=None) > before

    pgbackrest.restore(ctx, instance, date=before_drop)
    with instance_mod.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT datname FROM pg_database")
        assert "backrest" in [r[0] for r in rows]
