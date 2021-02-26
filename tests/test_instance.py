import subprocess

import pytest
from pgtoolkit.conf import parse as parse_pgconf

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
    pgroot = tmp_path / "pg"
    pgroot.mkdir()
    datadir = tmp_path / "notadirectory"
    datadir.touch()
    waldir = tmp_path / "wal2"
    with pytest.raises(subprocess.CalledProcessError):
        instance.init(
            datadir=datadir, waldir=waldir, surole="dbadmin", locale="C", pgroot=pgroot
        )
    assert not datadir.exists()
    assert not waldir.exists()
    assert not pgroot.exists()


def test_configure(tmp_path):
    instance_name = "test"
    postgresql_conf = tmp_path / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour = 'test'\n")
    initial_content = postgresql_conf.read_text()

    instance.configure(instance_name, configdir=tmp_path, filename="my.conf", port=5433)
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include = 'my.conf'"

    configfpath = tmp_path / "my.conf"
    lines = configfpath.read_text().splitlines()
    assert "port = 5433" in lines
    assert "cluster_name = 'test'" in lines

    with postgresql_conf.open() as f:
        config = parse_pgconf(f)
    assert config.port == 5433
    assert config.bonjour == "test"
    assert config.cluster_name == "test"

    instance.revert_configure(instance_name, configdir=tmp_path, filename="toto.conf")
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include = 'my.conf'"

    instance.revert_configure(instance_name, configdir=tmp_path, filename="my.conf")
    assert postgresql_conf.read_text() == initial_content
