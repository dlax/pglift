import contextlib
import shutil
from pathlib import Path

import pytest

from pglib import instance as instance_mod
from pglib import pgbackrest
from pglib.model import Instance


@pytest.fixture
def instance(tmp_settings, tmp_path):
    i = Instance("test", "11", settings=tmp_settings)
    pg_settings = tmp_settings.postgresql
    instance_mod.init(i, settings=pg_settings)
    instance_mod.configure(
        i, settings=pg_settings, unix_socket_directories=str(tmp_path)
    )
    return i


@contextlib.contextmanager
def instance_running(instance, tmp_path):
    logfile = tmp_path / "log"
    instance_mod.start(instance, logfile=logfile)
    try:
        yield
    finally:
        instance_mod.stop(instance)


@pytest.mark.skipif(
    shutil.which("pgbackrest") is None, reason="pgbackrest is not available"
)
def test(instance, tmp_settings, tmp_path):
    pgbackrest_settings = tmp_settings.pgbackrest

    kwargs = {"instance": instance, "settings": pgbackrest_settings}
    pgbackrest.setup(**kwargs)
    configpath = Path(pgbackrest_settings.configpath.format(instance=instance))
    directory = Path(pgbackrest_settings.directory.format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    assert "pg1-port = 5432" in lines
    assert directory.exists()

    with instance_running(instance, tmp_path):
        pgbackrest.init(instance, settings=pgbackrest_settings)
        assert (
            directory / f"archive/{instance.version}-{instance.name}/archive.info"
        ).exists()
        assert (
            directory / f"backup/{instance.version}-{instance.name}/backup.info"
        ).exists()

    pgbackrest.revert_setup(**kwargs)
    assert not configpath.exists()
    assert not directory.exists()
