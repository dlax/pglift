import pathlib
import re
from unittest.mock import patch

import attr
import pytest
from pgtoolkit.conf import parse as parse_pgconf

from pglift import exceptions
from pglift import instance as instance_mod
from pglift import task
from pglift.ctx import Context
from pglift.models import interface
from pglift.models.system import BaseInstance, Instance
from pglift.settings import Settings


def test_systemd_unit(pg_version: str, instance: Instance) -> None:
    assert (
        instance_mod.systemd_unit(instance)
        == f"pglift-postgresql@{pg_version}-test.service"
    )


def test_init_lookup_failed(pg_version: str, settings: Settings, ctx: Context) -> None:
    manifest = interface.Instance(name="dirty", version=pg_version)
    i = BaseInstance("dirty", pg_version, settings)
    i.datadir.mkdir(parents=True)
    (i.datadir / "postgresql.conf").touch()
    pg_version_file = i.datadir / "PG_VERSION"
    pg_version_file.write_text("7.1")
    with pytest.raises(Exception, match="version mismatch"):
        with task.transaction():
            instance_mod.init(ctx, manifest)
    assert not pg_version_file.exists()  # per revert


def test_init_dirty(
    pg_version: str, settings: Settings, ctx: Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = interface.Instance(name="dirty", version=pg_version)
    i = BaseInstance("dirty", pg_version, settings)
    i.datadir.mkdir(parents=True)
    (i.datadir / "dirty").touch()
    calls = []
    with pytest.raises(exceptions.CommandError):
        with task.transaction():
            with monkeypatch.context() as m:
                m.setattr("pglift.systemd.enable", lambda *a: calls.append(a))
                instance_mod.init(ctx, manifest)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()
    if ctx.settings.service_manager == "systemd":
        assert not calls


def test_init_version_not_available(ctx: Context) -> None:
    settings = ctx.settings
    version = "11"
    if pathlib.Path(settings.postgresql.bindir.format(version=version)).exists():
        pytest.skip(f"PostgreSQL {version} seems available")
    manifest = interface.Instance(name=f"pg{version}", version=version)
    with pytest.raises(EnvironmentError, match="pg_ctl executable not found"):
        instance_mod.init(ctx, manifest)


@pytest.mark.parametrize("data_checksums", [True, False])
def test_init_force_data_checksums(
    ctx: Context, pg_version: str, data_checksums: bool
) -> None:
    assert ctx.settings.postgresql.initdb.data_checksums is False
    manifest = interface.Instance(
        name="checksums", version=pg_version, data_checksums=data_checksums
    )
    with patch("pgtoolkit.ctl.PGCtl.init") as init:
        instance_mod.init(ctx, manifest)
    instance = BaseInstance.get(manifest.name, manifest.version, ctx)
    expected = {
        "waldir": str(instance.waldir),
        "username": "postgres",
        "encoding": "UTF8",
        "auth_local": "trust",
        "auth_host": "reject",
        "locale": "C",
    }
    if data_checksums:
        init.assert_called_once_with(instance.datadir, data_checksums=True, **expected)
    else:
        init.assert_called_once_with(instance.datadir, **expected)


def test_list_no_pgroot(ctx: Context) -> None:
    assert not ctx.settings.postgresql.root.exists()
    assert list(instance_mod.list(ctx)) == []


@pytest.mark.usefixtures("nohook")
def test_configure(
    ctx: Context, instance: Instance, instance_manifest: interface.Instance
) -> None:
    configdir = instance.datadir
    postgresql_conf = configdir / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour_name = 'overridden'\n")

    changes = instance_mod.configure(
        ctx,
        instance_manifest,
        values=dict(
            port=5433,
            max_connections=100,
            shared_buffers="10 %",
            effective_cache_size="5MB",
        ),
    )
    old_shared_buffers, new_shared_buffers = changes.pop("shared_buffers")
    assert old_shared_buffers is None
    assert new_shared_buffers is not None and new_shared_buffers != "10 %"
    assert changes == {
        "bonjour": (True, None),
        "bonjour_name": ("test", None),
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
    assert config.bonjour_name == "overridden"
    assert config.cluster_name == "test"

    changes = instance_mod.configure(
        ctx,
        instance_manifest,
        ssl=True,
        values=dict(port=None, listen_address="*"),
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
    changes = instance_mod.configure(
        ctx, instance_manifest, values=dict(listen_address="*"), ssl=True
    )
    assert changes == {}
    mtime_after = (
        postgresql_conf.stat().st_mtime,
        site_configfpath.stat().st_mtime,
        user_configfpath.stat().st_mtime,
    )
    assert mtime_before == mtime_after

    changes = instance_mod.configure(ctx, instance_manifest, ssl=True)
    lines = user_configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert (configdir / "server.crt").exists()
    assert (configdir / "server.key").exists()

    ssl = (cert_file, key_file) = (
        instance.datadir / "c.crt",
        instance.datadir / "k.key",
    )
    for fpath in ssl:
        fpath.touch()
    changes = instance_mod.configure(ctx, instance_manifest, ssl=ssl)
    assert changes == {
        "ssl_cert_file": (None, str(cert_file)),
        "ssl_key_file": (None, str(key_file)),
    }
    lines = user_configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert f"ssl_cert_file = '{instance.datadir / 'c.crt'}'" in lines
    assert f"ssl_key_file = '{instance.datadir / 'k.key'}'" in lines
    for fpath in ssl:
        assert fpath.exists()

    # reconfigure default ssl certs
    changes = instance_mod.configure(ctx, instance_manifest, ssl=True)
    assert changes == {
        "ssl_cert_file": (str(cert_file), None),
        "ssl_key_file": (str(key_file), None),
    }

    # disable ssl
    changes = instance_mod.configure(ctx, instance_manifest, ssl=False)
    assert changes == {
        "ssl": (True, None),
    }


def test_check_status(ctx: Context, instance: Instance) -> None:
    with pytest.raises(exceptions.InstanceStateError, match="instance is not_running"):
        instance_mod.check_status(ctx, instance, instance_mod.Status.running)
    instance_mod.check_status(ctx, instance, instance_mod.Status.not_running)


def test_start_foreground(ctx: Context, instance: Instance) -> None:
    with patch("os.execv") as execv:
        instance_mod.start(ctx, instance, foreground=True)
    postgres = ctx.pg_ctl(instance.version).bindir / "postgres"
    execv.assert_called_once_with(
        str(postgres), f"{postgres} -D {instance.datadir}".split()
    )


def test_drop(
    ctx: Context, instance: Instance, caplog: pytest.LogCaptureFixture
) -> None:
    with patch.object(ctx, "confirm", return_value=False) as confirm:
        with pytest.raises(exceptions.Cancelled):
            instance_mod.drop(ctx, instance)
    confirm.assert_called_once_with(
        f"Confirm complete deletion of instance {instance}?", True
    )


def test_env_for(ctx: Context, instance: Instance) -> None:
    assert instance_mod.env_for(ctx, instance) == {
        "PGDATA": str(instance.datadir),
        "PGHOST": "/socks",
        "PGPASSFILE": str(ctx.settings.postgresql.auth.passfile),
        "PGPORT": "999",
        "PGUSER": "postgres",
    }


def test_exec(ctx: Context, instance: Instance) -> None:
    with patch("os.execve") as patched, patch.dict(
        "os.environ", {"PGPASSWORD": "qwerty"}, clear=True
    ):
        instance_mod.exec(
            ctx, instance, command=("psql", "--user", "test", "--dbname", "test")
        )
    expected_env = {
        "PGDATA": str(instance.datadir),
        "PGPASSFILE": str(ctx.settings.postgresql.auth.passfile),
        "PGPORT": "999",
        "PGUSER": "postgres",
        "PGHOST": "/socks",
        "PGPASSWORD": "qwerty",
    }
    bindir = ctx.pg_ctl(instance.version).bindir
    cmd = [
        f"{bindir}/psql",
        "--user",
        "test",
        "--dbname",
        "test",
    ]
    patched.assert_called_once_with(f"{bindir}/psql", cmd, expected_env)


def test_env(ctx: Context, instance: Instance) -> None:
    bindir = ctx.pg_ctl(instance.version).bindir
    with patch.dict("os.environ", {"PATH": "/pg10/bin"}):
        assert instance_mod.env(ctx, instance) == "\n".join(
            [
                f"export PATH={bindir}:/pg10/bin",
                f"export PGDATA={instance.datadir}",
                "export PGHOST=/socks",
                f"export PGPASSFILE={ctx.settings.postgresql.auth.passfile}",
                "export PGPORT=999",
                "export PGUSER=postgres",
            ]
        )


def test_exists(ctx: Context, instance: Instance) -> None:
    assert instance_mod.exists(ctx, instance.name, instance.version)
    assert not instance_mod.exists(ctx, "doesnotexists", instance.version)


def test_upgrade_forbid_same_instance(ctx: Context, instance: Instance) -> None:
    with pytest.raises(
        exceptions.InvalidVersion,
        match=f"Could not upgrade {instance.version}/test using same name and same version",
    ):
        instance_mod.upgrade(ctx, instance, version=instance.version)


def test_upgrade_target_instance_exists(ctx: Context, instance: Instance) -> None:
    orig_instance = attr.evolve(instance, name="old")
    with pytest.raises(exceptions.InstanceAlreadyExists):
        instance_mod.upgrade(
            ctx, orig_instance, version=instance.version, name=instance.name
        )


def test_upgrade_confirm(ctx: Context, instance: Instance, pg_version: str) -> None:
    with patch.object(ctx, "confirm", return_value=False) as confirm:
        with pytest.raises(exceptions.Cancelled):
            instance_mod.upgrade(ctx, instance, name="new")
    confirm.assert_called_once_with(
        f"Confirm upgrade of instance {instance} to version {pg_version}?",
        True,
    )


def test_standby_upgrade(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        instance_mod.upgrade(
            ctx, standby_instance, version=str(int(standby_instance.version) + 1)
        )


def test_non_standby_promote(ctx: Context, instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceStateError,
        match=f"^{instance.version}/test is not a standby$",
    ):
        instance_mod.promote(ctx, instance)


def test_logs(ctx: Context, instance: Instance, tmp_path: pathlib.Path) -> None:
    with pytest.raises(
        exceptions.FileNotFoundError,
        match=r"file 'current_logfiles' for instance \d{2}/test not found",
    ):
        next(instance_mod.logs(ctx, instance))

    current_logfiles = instance.datadir / "current_logfiles"
    current_logfiles.write_text("csvlog log/postgresql.csv\n")
    with pytest.raises(ValueError, match="no record matching 'stderr'"):
        next(instance_mod.logs(ctx, instance))

    stderr_logpath = tmp_path / "postgresql.log"
    current_logfiles.write_text(f"stderr {stderr_logpath}\n")
    with pytest.raises(exceptions.SystemError, match="failed to read"):
        next(instance_mod.logs(ctx, instance))

    stderr_logpath.write_text("line1\nline2\n")
    assert list(instance_mod.logs(ctx, instance)) == ["line1\n", "line2\n"]
