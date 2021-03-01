import subprocess

import attr
import pytest
from pgtoolkit.conf import parse as parse_pgconf

from pglib import instance, settings
from pglib.model import Instance


@pytest.fixture
def tmp_settings(tmp_path):
    return settings.Settings(postgresql=settings.PostgreSQLSettings(root=tmp_path))


def test_init(tmp_settings):
    pgroot = tmp_settings.postgresql.root
    i = Instance("test", "13", 5432, settings=tmp_settings)
    instance.init(i, data_checksums=True, settings=tmp_settings.postgresql)
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

    # A failed init cleans up postgres directories.
    pgroot = tmp_settings.postgresql.root
    tmp_settings_1 = attr.evolve(
        tmp_settings,
        postgresql=attr.evolve(tmp_settings.postgresql, root=pgroot / "pg"),
    )
    pgroot = pgroot / "pg"
    pgroot.mkdir()
    i = Instance("test", "12", 5433, settings=tmp_settings_1)
    i.datadir.mkdir(parents=True)
    (i.datadir / "dirty").touch()
    with pytest.raises(subprocess.CalledProcessError):
        instance.init(i, settings=tmp_settings_1.postgresql)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()


def test_configure(tmp_settings):
    i = Instance("test", "11", 5433, settings=tmp_settings)
    configdir = i.datadir
    configdir.mkdir(parents=True)
    postgresql_conf = i.datadir / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour = 'test'\n")
    initial_content = postgresql_conf.read_text()

    instance.configure(i, filename="my.conf", port=5433)
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

    instance.revert_configure(i, filename="toto.conf")
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include = 'my.conf'"

    instance.revert_configure(i, filename="my.conf")
    assert postgresql_conf.read_text() == initial_content

    instance.configure(i, filename="ssl.conf", ssl=True)
    configfpath = configdir / "ssl.conf"
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert (configdir / "server.crt").exists()
    assert (configdir / "server.key").exists()

    instance.revert_configure(i, filename="ssl.conf", ssl=True)
    assert not (configdir / "server.crt").exists()
    assert not (configdir / "server.key").exists()

    ssl = (i.datadir / "c.crt", i.datadir / "k.key")
    for fpath in ssl:
        fpath.touch()
    instance.configure(i, filename="ssl.conf", ssl=ssl)
    configfpath = configdir / "ssl.conf"
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert f"ssl_cert_file = {i.datadir / 'c.crt'}" in lines
    assert f"ssl_key_file = {i.datadir / 'k.key'}" in lines
    instance.revert_configure(i, filename="ssl.conf", ssl=ssl)
    for fpath in ssl:
        assert fpath.exists()
