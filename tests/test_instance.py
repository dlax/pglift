import subprocess

import pytest

from pglib import instance


def test_init(tmp_path):
    pgroot = tmp_path
    datadir = tmp_path / "data"
    waldir = tmp_path / "wal"
    instance.init(
        datadir=datadir,
        waldir=waldir,
        surole="dbadmin",
        locale="C",
        pgroot=pgroot,
        data_checksums=True,
    )
    assert datadir.exists()
    assert waldir.exists()
    postgresql_conf = datadir / "postgresql.conf"
    assert postgresql_conf.exists()
    assert (waldir / "archive_status").is_dir()
    with postgresql_conf.open() as f:
        for line in f:
            if "lc_messages = 'C'" in line:
                break
        else:
            raise AssertionError("invalid postgresql.conf")

    # A failed init cleans up postgres directories.
    datadir = tmp_path / "notadirectory"
    datadir.touch()
    waldir = tmp_path / "wal2"
    with pytest.raises(subprocess.CalledProcessError):
        instance.init(
            datadir=datadir, waldir=waldir, surole="dbadmin", locale="C", pgroot=pgroot
        )
    assert not datadir.exists()
    assert not waldir.exists()
