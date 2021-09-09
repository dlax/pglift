import pathlib
import re
from unittest.mock import patch

import pytest
from pgtoolkit.conf import parse as parse_pgconf

from pglift import instance as instance_mod
from pglift import task
from pglift.exceptions import CommandError, InstanceStateError
from pglift.models.system import InstanceSpec, PrometheusService


def test_systemd_unit(pg_version, instance):
    assert (
        instance_mod.systemd_unit(instance) == f"postgresql@{pg_version}-test.service"
    )


def test_init_lookup_failed(pg_version, settings, ctx):
    i = InstanceSpec(
        name="dirty",
        version=pg_version,
        settings=settings,
        prometheus=PrometheusService(),
        standby=None,
    )
    i.datadir.mkdir(parents=True)
    (i.datadir / "postgresql.conf").touch()
    pg_version = i.datadir / "PG_VERSION"
    pg_version.write_text("7.1")
    with pytest.raises(Exception, match="version mismatch"):
        with task.runner(ctx):
            instance_mod.init(ctx, i)
    assert not pg_version.exists()  # per revert


def test_init_dirty(pg_version, settings, ctx, monkeypatch):
    i = InstanceSpec(
        name="dirty",
        version=pg_version,
        settings=settings,
        prometheus=PrometheusService(),
        standby=None,
    )
    i.datadir.mkdir(parents=True)
    (i.datadir / "dirty").touch()
    calls = []
    with pytest.raises(CommandError):
        with task.runner(ctx):
            with monkeypatch.context() as m:
                m.setattr("pglift.systemd.enable", lambda *a: calls.append(a))
                instance_mod.init(ctx, i)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()
    if ctx.settings.service_manager == "systemd":
        assert not calls


def test_init_version_not_available(ctx):
    settings = ctx.settings
    version = "10"
    if pathlib.Path(settings.postgresql.bindir.format(version=version)).exists():
        pytest.skip(f"PostgreSQL {version} seems available")
    i = InstanceSpec(
        f"pg{version}",
        version,
        settings=settings,
        prometheus=PrometheusService(),
        standby=None,
    )
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

    changes = instance_mod.configure(
        ctx,
        instance,
        port=5433,
        max_connections=100,
        shared_buffers="10 %",
        effective_cache_size="5MB",
    )
    old_shared_buffers, new_shared_buffers = changes.pop("shared_buffers")
    assert old_shared_buffers is None
    assert new_shared_buffers is not None and new_shared_buffers != "10 %"
    assert changes == {
        "effective_cache_size": (None, "5MB"),
        "max_connections": (None, 100),
        "port": (None, 5433),
    }
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include_dir = 'conf.pglift.d'"

    site_configfpath = configdir / "conf.pglift.d" / "site.conf"
    user_configfpath = configdir / "conf.pglift.d" / "user.conf"
    lines = user_configfpath.read_text().splitlines()
    assert "port = 5433" in lines
    site_config = site_configfpath.read_text()
    assert "cluster_name = 'test'" in site_config.splitlines()
    assert re.search(r"shared_buffers = '\d+ [kMGT]?B'", site_config)
    assert "effective_cache_size" in site_config
    assert (
        f"unix_socket_directories = '{ctx.settings.prefix}/run/postgresql'"
        in site_config
    )

    with postgresql_conf.open() as f:
        config = parse_pgconf(f)
    assert config.port == 5433
    assert config.bonjour_name == "test"
    assert config.cluster_name == "test"

    changes = instance_mod.configure(
        ctx, instance, listen_address="*", ssl=True, port=None
    )
    assert changes == {
        "effective_cache_size": ("5MB", None),
        "listen_address": (None, "*"),
        "max_connections": (100, None),
        "port": (5433, None),
        "shared_buffers": (new_shared_buffers, None),
        "ssl": (None, True),
    }
    # Same configuration, no change.
    mtime_before = (
        postgresql_conf.stat().st_mtime,
        site_configfpath.stat().st_mtime,
        user_configfpath.stat().st_mtime,
    )
    changes = instance_mod.configure(ctx, instance, listen_address="*", ssl=True)
    assert changes == {}
    mtime_after = (
        postgresql_conf.stat().st_mtime,
        site_configfpath.stat().st_mtime,
        user_configfpath.stat().st_mtime,
    )
    assert mtime_before == mtime_after

    instance_mod.revert_configure(ctx, instance)
    assert postgresql_conf.read_text() == initial_content
    assert not site_configfpath.exists()
    assert not user_configfpath.exists()

    instance_mod.configure(ctx, instance, ssl=True)
    lines = user_configfpath.read_text().splitlines()
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
        "ssl": (None, True),
        "ssl_cert_file": (None, cert_file),
        "ssl_key_file": (None, key_file),
    }
    lines = user_configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert f"ssl_cert_file = {instance.datadir / 'c.crt'}" in lines
    assert f"ssl_key_file = {instance.datadir / 'k.key'}" in lines
    instance_mod.revert_configure(ctx, instance, ssl=ssl)
    for fpath in ssl:
        assert fpath.exists()


def test_check_status(ctx, instance):
    with pytest.raises(InstanceStateError, match="instance is not_running"):
        instance_mod.check_status(ctx, instance, instance_mod.Status.running)
    instance_mod.check_status(ctx, instance, instance_mod.Status.not_running)


def test_shell(ctx, instance):
    with patch("os.execvp") as patched:
        instance_mod.shell(ctx, instance, user="test", dbname="test")
    psql = str(ctx.pg_ctl(instance.version).bindir / "psql")
    cmd = [
        psql,
        "--port",
        str(instance.port),
        "--host",
        "/socks",
        "--user",
        "test",
        "--dbname",
        "test",
    ]
    patched.assert_called_once_with(psql, cmd)
