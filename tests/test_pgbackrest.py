import contextlib
import shutil
from pathlib import Path

import pytest

from pglib import instance as instance_mod
from pglib import pgbackrest
from pglib.conf import info as conf_info
from pglib.model import Instance


@pytest.fixture
def instance(ctx, tmp_settings, tmp_path):
    i = Instance.default_version("test", settings=tmp_settings, ctx=ctx)
    pg_settings = tmp_settings.postgresql
    instance_mod.init(ctx, i, settings=pg_settings)
    instance_mod.configure(
        ctx, i, settings=pg_settings, unix_socket_directories=str(tmp_path)
    )
    return i


@contextlib.contextmanager
def instance_running(ctx, instance):
    instance_mod.start(ctx, instance)
    try:
        yield
    finally:
        instance_mod.stop(ctx, instance)


@pytest.mark.skipif(
    shutil.which("pgbackrest") is None, reason="pgbackrest is not available"
)
def test(ctx, instance, tmp_settings, tmp_path):
    pgbackrest_settings = tmp_settings.pgbackrest

    kwargs = {"instance": instance, "settings": pgbackrest_settings}
    pgbackrest.setup(ctx, **kwargs)
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
        pgbackrest.init(ctx, instance, settings=pgbackrest_settings)
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
            settings=pgbackrest_settings,
        )
        assert latest_backup.exists() and latest_backup.is_symlink()
        pgbackrest.expire(ctx, instance, settings=pgbackrest_settings)
        # TODO: check some result from 'expire' command here.

    # Calling setup an other time doesn't overwrite configuration
    configdir = instance.datadir
    pgconfigfile = conf_info(configdir, name="pgbackrest.conf")[1]
    mtime_before = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    pgbackrest.setup(ctx, **kwargs)
    mtime_after = configpath.stat().st_mtime, pgconfigfile.stat().st_mtime
    assert mtime_before == mtime_after

    pgbackrest.revert_setup(ctx, **kwargs)
    assert not configpath.exists()
    assert not directory.exists()
