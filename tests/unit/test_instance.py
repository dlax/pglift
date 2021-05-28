import subprocess

import pytest
from pgtoolkit.conf import parse as parse_pgconf

from pglift import instance as instance_mod
from pglift import task
from pglift.model import Instance


def test_init_lookup_failed(ctx):
    i = Instance.default_version("dirty", ctx=ctx)
    i.datadir.mkdir(parents=True)
    (i.datadir / "postgresql.conf").touch()
    pg_version = i.datadir / "PG_VERSION"
    pg_version.write_text("7.1")
    with pytest.raises(Exception, match="version mismatch"):
        with task.runner():
            instance_mod.init(ctx, i)
    assert not pg_version.exists()  # per revert


def test_init_dirty(ctx, monkeypatch):
    i = Instance.default_version("dirty", ctx=ctx)
    i.datadir.mkdir(parents=True)
    (i.datadir / "dirty").touch()
    calls = []
    with pytest.raises(subprocess.CalledProcessError):
        with task.runner():
            with monkeypatch.context() as m:
                m.setattr("pglift.systemd.enable", lambda *a: calls.append(a))
                instance_mod.init(ctx, i)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()
    if ctx.settings.service_manager == "systemd":
        assert not calls


def test_init_version_not_available(ctx):
    i = Instance("pg96", "9.6", settings=ctx.settings)
    with pytest.raises(EnvironmentError, match="pg_ctl executable not found"):
        instance_mod.init(ctx, i)


@pytest.fixture
def ctx_nohook(ctx):
    ctx.pm.unregister_all()
    return ctx


def test_configure(ctx_nohook, instance):
    ctx = ctx_nohook
    configdir = instance.datadir
    postgresql_conf = configdir / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour_name = 'test'\n")
    initial_content = postgresql_conf.read_text()

    changes = instance_mod.configure(ctx, instance, port=5433, max_connections=100)
    assert changes == {
        "cluster_name": (None, "test"),
        "max_connections": (None, 100),
        "port": (None, 5433),
    }
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include_dir = 'conf.pglift.d'"

    configfpath = configdir / "conf.pglift.d" / "user.conf"
    lines = configfpath.read_text().splitlines()
    assert "port = 5433" in lines
    assert "cluster_name = 'test'" in lines

    with postgresql_conf.open() as f:
        config = parse_pgconf(f)
    assert config.port == 5433
    assert config.bonjour_name == "test"
    assert config.cluster_name == "test"

    changes = instance_mod.configure(ctx, instance, listen_address="*", ssl=True)
    assert changes == {
        "listen_address": (None, "*"),
        "max_connections": (100, None),
        "port": (5433, None),
        "ssl": (None, True),
    }
    # Same configuration, no change.
    mtime_before = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    changes = instance_mod.configure(ctx, instance, listen_address="*", ssl=True)
    assert changes == {}
    mtime_after = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    assert mtime_before == mtime_after

    instance_mod.revert_configure(ctx, instance)
    assert postgresql_conf.read_text() == initial_content
    assert not configfpath.exists()

    instance_mod.configure(ctx, instance, ssl=True)
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert (configdir / "server.crt").exists()
    assert (configdir / "server.key").exists()

    instance_mod.revert_configure(ctx, instance, ssl=True)
    assert not (configdir / "server.crt").exists()
    assert not (configdir / "server.key").exists()

    ssl = (cert_file, key_file) = (
        instance.datadir / "c.crt",
        instance.datadir / "k.key",
    )
    for fpath in ssl:
        fpath.touch()
    changes = instance_mod.configure(ctx, instance, ssl=ssl)
    assert changes == {
        "cluster_name": (None, instance.name),
        "ssl": (None, True),
        "ssl_cert_file": (None, cert_file),
        "ssl_key_file": (None, key_file),
    }
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert f"ssl_cert_file = {instance.datadir / 'c.crt'}" in lines
    assert f"ssl_key_file = {instance.datadir / 'k.key'}" in lines
    instance_mod.revert_configure(ctx, instance, ssl=ssl)
    for fpath in ssl:
        assert fpath.exists()