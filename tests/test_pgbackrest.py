import shutil
from pathlib import Path

import pytest

from pglib import instance as instance_mod
from pglib import pgbackrest
from pglib.conf import info as conf_info

from . import instance_running


@pytest.fixture
def ctx(ctx):
    ctx.pm.unregister(pgbackrest)
    return ctx


@pytest.mark.skipif(
    shutil.which("pgbackrest") is None, reason="pgbackrest is not available"
)
def test(ctx, instance, tmp_path):
    pgbackrest_settings = ctx.settings.pgbackrest

    pgbackrest.setup(ctx, instance)
    configpath = Path(pgbackrest_settings.configpath.format(instance=instance))
    directory = Path(pgbackrest_settings.directory.format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    assert "pg1-port = 5432" in lines
    assert directory.exists()

    latest_backup = (
        directory / "backup" / f"{instance.version}-{instance.name}" / "latest"
    )

    with instance_running(ctx, instance):
        pgbackrest.init(ctx, instance)
        assert (
            directory / f"archive/{instance.version}-{instance.name}/archive.info"
        ).exists()
        assert (
            directory / f"backup/{instance.version}-{instance.name}/backup.info"
        ).exists()

        assert not latest_backup.exists()
        pgbackrest.backup(
            ctx,
            instance,
            type=pgbackrest.BackupType.full,
        )
        assert latest_backup.exists() and latest_backup.is_symlink()
        pgbackrest.expire(ctx, instance)
        # TODO: check some result from 'expire' command here.

    # Calling setup an other time doesn't overwrite configuration
    configdir = instance.datadir
    pgconfigfile = conf_info(configdir, name="pgbackrest.conf")[1]
    mtime_before = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    pgbackrest.setup(ctx, instance)
    mtime_after = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    assert mtime_before == mtime_after

    # If instance's configuration changes, pgbackrest configuration is
    # updated.
    config_before = configpath.read_text()
    instance_mod.configure(ctx, instance, port=5555)
    pgbackrest.setup(ctx, instance)
    config_after = configpath.read_text()
    assert config_after != config_before
    assert "pg1-port = 5555" in config_after.splitlines()

    pgbackrest.revert_setup(ctx, instance)
    assert not configpath.exists()
    assert not directory.exists()
