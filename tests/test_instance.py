import subprocess

import attr
import pytest
from pgtoolkit.conf import parse as parse_pgconf

from pglib import instance, pg, settings
from pglib.model import Instance


@pytest.fixture
def tmp_settings(tmp_path):
    return settings.to_config(
        {
            "PGLIB_POSTGRESQL_ROOT": str(tmp_path),
            "PGLIB_POSTGRESQL_CONFIG_FILE": "my.conf",
        },
    )


def test_init(tmp_settings):
    pgroot = tmp_settings.postgresql.root
    i = Instance("test", "11", settings=tmp_settings)
    ret = instance.init(i, data_checksums=True, settings=tmp_settings.postgresql)
    assert ret
    assert i.datadir.exists()
    assert i.waldir.exists()
    postgresql_conf = i.datadir / "postgresql.conf"
    assert postgresql_conf.exists()
    assert (i.waldir / "archive_status").is_dir()
    with postgresql_conf.open() as f:
        for line in f:
            if "lc_messages = 'C'" in line:
                break
        else:
            raise AssertionError("invalid postgresql.conf")

    # Instance alread exists, no-op.
    ret = instance.init(i, settings=tmp_settings.postgresql)
    assert not ret

    # Lookup failed.
    pg_version = i.datadir / "PG_VERSION"
    pg_version.write_text("7.1")
    with pytest.raises(
        Exception,
        match="version mismatch",
    ):
        instance.init(i, settings=tmp_settings.postgresql)
    assert not pg_version.exists()  # per revert

    # A failed init cleans up postgres directories.
    pgroot = tmp_settings.postgresql.root
    tmp_settings_1 = attr.evolve(
        tmp_settings,
        postgresql=attr.evolve(tmp_settings.postgresql, root=pgroot / "pg"),
    )
    pgroot = pgroot / "pg"
    pgroot.mkdir()
    i = Instance("test", "12", settings=tmp_settings_1)
    i.datadir.mkdir(parents=True)
    (i.datadir / "dirty").touch()
    with pytest.raises(subprocess.CalledProcessError):
        instance.init(i, settings=tmp_settings_1.postgresql)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()


def test_configure(tmp_settings):
    pg_settings = tmp_settings.postgresql
    i = Instance("test", "11", settings=tmp_settings)
    configdir = i.datadir
    configdir.mkdir(parents=True)
    postgresql_conf = i.datadir / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour = 'test'\n")
    initial_content = postgresql_conf.read_text()

    changes = instance.configure(
        i, port=5433, settings=pg_settings, max_connections=100
    )
    assert changes == {
        "cluster_name": (None, "test"),
        "max_connections": (None, 100),
        "port": (None, 5433),
    }
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include = 'my.conf'"

    configfpath = configdir / "my.conf"
    lines = configfpath.read_text().splitlines()
    assert "port = 5433" in lines
    assert "cluster_name = 'test'" in lines

    with postgresql_conf.open() as f:
        config = parse_pgconf(f)
    assert config.port == 5433
    assert config.bonjour == "test"
    assert config.cluster_name == "test"

    changes = instance.configure(i, settings=pg_settings, listen_address="*", ssl=True)
    assert changes == {
        "listen_address": (None, "*"),
        "max_connections": (100, None),
        "port": (5433, None),
        "ssl": (None, True),
    }
    # Same configuration, no change.
    mtime_before = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    changes = instance.configure(i, settings=pg_settings, listen_address="*", ssl=True)
    assert changes == {}
    mtime_after = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    assert mtime_before == mtime_after

    instance.revert_configure(
        i, settings=attr.evolve(pg_settings, config_file="foo.conf")
    )
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include = 'my.conf'"

    instance.revert_configure(i, settings=pg_settings)
    assert postgresql_conf.read_text() == initial_content

    pg_settings = attr.evolve(pg_settings, config_file="ssl.conf")
    instance.configure(i, ssl=True, settings=pg_settings)
    configfpath = configdir / "ssl.conf"
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert (configdir / "server.crt").exists()
    assert (configdir / "server.key").exists()

    instance.revert_configure(i, ssl=True, settings=pg_settings)
    assert not (configdir / "server.crt").exists()
    assert not (configdir / "server.key").exists()

    ssl = (cert_file, key_file) = (i.datadir / "c.crt", i.datadir / "k.key")
    for fpath in ssl:
        fpath.touch()
    changes = instance.configure(i, ssl=ssl, settings=pg_settings)
    assert changes == {
        "cluster_name": (None, i.name),
        "ssl": (None, True),
        "ssl_cert_file": (None, cert_file),
        "ssl_key_file": (None, key_file),
    }
    configfpath = configdir / "ssl.conf"
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert f"ssl_cert_file = {i.datadir / 'c.crt'}" in lines
    assert f"ssl_key_file = {i.datadir / 'k.key'}" in lines
    instance.revert_configure(i, ssl=ssl, settings=pg_settings)
    for fpath in ssl:
        assert fpath.exists()


def test_start_stop(tmp_settings, tmp_path):
    pg_settings = tmp_settings.postgresql
    i = Instance("test", "11", settings=tmp_settings)
    assert instance.status(i) == pg.Status.UNSPECIFIED_DATADIR

    instance.init(i, settings=pg_settings)
    instance.configure(
        i,
        port=5499,
        log_destination="syslog",
        unix_socket_directories=str(tmp_path),
        settings=pg_settings,
    )
    assert instance.status(i) == pg.Status.NOT_RUNNING

    instance.start(i, logfile=tmp_path / "log")
    try:
        assert instance.status(i) == pg.Status.RUNNING
    finally:
        instance.stop(i)
    assert instance.status(i) == pg.Status.NOT_RUNNING
