import logging
import pathlib
import re
import stat
from typing import Any
from unittest.mock import patch

import attr
import pytest
from pgtoolkit.conf import parse as parse_pgconf

from pglift import exceptions, instances, task
from pglift.ctx import Context
from pglift.models import interface
from pglift.models.system import BaseInstance, Instance
from pglift.settings import Settings
from pglift.types import ConfigChanges


def test_systemd_unit(pg_version: str, instance: Instance) -> None:
    assert (
        instances.systemd_unit(instance)
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
            instances.init(ctx, manifest)
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
                instances.init(ctx, manifest)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()
    if ctx.settings.service_manager == "systemd":
        assert not calls


def test_init_version_not_available(ctx: Context) -> None:
    settings = ctx.settings
    version = "11"
    if (
        settings.postgresql.bindir
        and pathlib.Path(settings.postgresql.bindir.format(version=version)).exists()
    ):
        pytest.skip(f"PostgreSQL {version} seems available")
    manifest = interface.Instance(name=f"pg{version}", version=version)
    with pytest.raises(EnvironmentError, match="pg_ctl executable not found"):
        instances.init(ctx, manifest)


@pytest.mark.parametrize("data_checksums", [True, False])
def test_init_force_data_checksums(
    ctx: Context, pg_version: str, data_checksums: bool
) -> None:
    assert ctx.settings.postgresql.initdb.data_checksums is None
    manifest = interface.Instance(
        name="checksums", version=pg_version, data_checksums=data_checksums
    )
    instance = BaseInstance.get(manifest.name, manifest.version, ctx)

    def fake_init(*a: Any, **kw: Any) -> None:
        instance.datadir.mkdir(parents=True)
        (instance.datadir / "postgresql.conf").touch()

    with patch("pgtoolkit.ctl.PGCtl.init", side_effect=fake_init) as init:
        instances.init(ctx, manifest)
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
    assert list(instances.list(ctx)) == []


@pytest.mark.usefixtures("nohook")
def test_configure(
    ctx: Context, instance: Instance, instance_manifest: interface.Instance
) -> None:
    assert not instance_manifest.ssl
    configdir = instance.datadir
    postgresql_conf = configdir / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour_name = 'overridden'\n")

    changes = instances.configure(
        ctx,
        instance_manifest._copy_validate(
            {
                "configuration": dict(
                    instance_manifest.configuration,
                    max_connections=100,
                    shared_buffers="10 %",
                    effective_cache_size="5MB",
                ),
                "port": 5433,
            }
        ),
    )
    old_shared_buffers, new_shared_buffers = changes.pop("shared_buffers")
    assert old_shared_buffers is None
    assert new_shared_buffers is not None and new_shared_buffers != "10 %"
    assert changes == {
        "bonjour_name": ("overridden", None),
        "cluster_name": (None, "test"),
        "effective_cache_size": (None, "5MB"),
        "lc_messages": (None, "C"),
        "lc_monetary": (None, "C"),
        "lc_numeric": (None, "C"),
        "lc_time": (None, "C"),
        "log_destination": (None, "stderr"),
        "logging_collector": (None, True),
        "max_connections": (None, 100),
        "port": (None, 5433),
        "shared_preload_libraries": (None, "passwordcheck"),
        "unix_socket_directories": (
            None,
            str(ctx.settings.postgresql.socket_directory),
        ),
    }

    postgresql_conf = configdir / "postgresql.conf"
    content = postgresql_conf.read_text()
    lines = content.splitlines()
    assert "port = 5433" in lines
    assert "cluster_name = 'test'" in lines
    assert re.search(r"shared_buffers = '\d+ [kMGT]?B'", content)
    assert "effective_cache_size" in content
    assert (
        f"unix_socket_directories = '{ctx.settings.prefix}/run/postgresql'" in content
    )

    with postgresql_conf.open() as f:
        config = parse_pgconf(f)
    assert config.port == 5433
    assert config.entries["bonjour_name"].commented
    assert config.cluster_name == "test"

    changes = instances.configure(
        ctx,
        instance_manifest._copy_validate(
            {
                "configuration": dict(
                    instance_manifest.configuration, listen_address="*"
                ),
                "port": 5432,
                "ssl": True,
            }
        ),
    )
    old_effective_cache_size, new_effective_cache_size = changes.pop(
        "effective_cache_size"
    )
    assert old_effective_cache_size == "5MB"
    assert new_effective_cache_size != old_effective_cache_size
    old_shared_buffers1, new_shared_buffers1 = changes.pop("shared_buffers")
    assert old_shared_buffers1 == new_shared_buffers
    assert new_shared_buffers1 != old_shared_buffers1
    assert changes == {
        "listen_address": (None, "*"),
        "max_connections": (100, None),
        "port": (5433, 5432),
        "ssl": (None, True),
    }

    # Same configuration, no change.
    mtime_before = postgresql_conf.stat().st_mtime
    changes = instances.configure(
        ctx,
        instance_manifest._copy_validate(
            {
                "configuration": dict(
                    instance_manifest.configuration, listen_address="*"
                ),
                "ssl": True,
            }
        ),
    )
    assert changes == {}
    mtime_after = postgresql_conf.stat().st_mtime
    assert mtime_before == mtime_after

    changes = instances.configure(ctx, instance_manifest._copy_validate({"ssl": True}))
    lines = postgresql_conf.read_text().splitlines()
    assert "ssl = on" in lines
    crt = configdir / "server.crt"
    key = configdir / "server.key"
    assert crt.exists()
    assert key.exists()
    assert stat.filemode(crt.stat().st_mode) == "-rw-r--r--"
    assert stat.filemode(key.stat().st_mode) == "-rw-------"

    ssl = (cert_file, key_file) = (
        instance.datadir / "c.crt",
        instance.datadir / "k.key",
    )
    for fpath in ssl:
        fpath.touch()
    changes = instances.configure(ctx, instance_manifest._copy_validate({"ssl": ssl}))
    assert changes == {
        "ssl_cert_file": (None, str(cert_file)),
        "ssl_key_file": (None, str(key_file)),
    }
    lines = postgresql_conf.read_text().splitlines()
    assert "ssl = on" in lines
    assert f"ssl_cert_file = '{instance.datadir / 'c.crt'}'" in lines
    assert f"ssl_key_file = '{instance.datadir / 'k.key'}'" in lines
    for fpath in ssl:
        assert fpath.exists()

    # reconfigure default ssl certs
    changes = instances.configure(ctx, instance_manifest._copy_validate({"ssl": True}))
    assert changes == {
        "ssl_cert_file": (str(cert_file), None),
        "ssl_key_file": (str(key_file), None),
    }

    # disable ssl
    changes = instances.configure(ctx, instance_manifest)
    assert changes == {
        "ssl": (True, None),
    }


def test_configure_auth(
    settings: Settings, instance_manifest: interface.Instance, instance: Instance
) -> None:
    hba = instance.datadir / "pg_hba.conf"
    ident = instance.datadir / "pg_ident.conf"
    orig_hba = hba.read_text()
    orig_ident = ident.read_text()
    instances.configure_auth(settings, instance, instance_manifest)
    assert hba.read_text() != orig_hba
    assert ident.read_text() != orig_ident


def test_check_status(ctx: Context, instance: Instance) -> None:
    with pytest.raises(exceptions.InstanceStateError, match="instance is not_running"):
        instances.check_status(ctx, instance, instances.Status.running)
    instances.check_status(ctx, instance, instances.Status.not_running)


def test_start_foreground(ctx: Context, instance: Instance) -> None:
    with patch("os.execv") as execv:
        instances.start(ctx, instance, foreground=True)
    postgres = instances.pg_ctl(instance.version, ctx=ctx).bindir / "postgres"
    execv.assert_called_once_with(
        str(postgres), f"{postgres} -D {instance.datadir}".split()
    )


def test_drop(
    ctx: Context, instance: Instance, caplog: pytest.LogCaptureFixture
) -> None:
    with patch.object(ctx, "confirm", return_value=False) as confirm:
        with pytest.raises(exceptions.Cancelled):
            instances.drop(ctx, instance)
    confirm.assert_called_once_with(
        f"Confirm complete deletion of instance {instance}?", True
    )


def test_env_for(ctx: Context, instance: Instance) -> None:
    expected_env = {
        "PGDATA": str(instance.datadir),
        "PGHOST": "/socks",
        "PGPASSFILE": str(ctx.settings.postgresql.auth.passfile),
        "PGPORT": "999",
        "PGUSER": "postgres",
        "PSQLRC": f"{instance.path}/.psqlrc",
        "PSQL_HISTORY": f"{instance.path}/.psql_history",
        "PGBACKREST_CONFIG": f"{ctx.settings.prefix}/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf",
        "PGBACKREST_STANZA": f"{instance.version}-{instance.name}",
    }
    assert instances.env_for(ctx, instance) == expected_env


def test_exec(ctx: Context, instance: Instance) -> None:
    with patch("os.execve") as patched, patch.dict(
        "os.environ", {"PGPASSWORD": "qwerty"}, clear=True
    ):
        instances.exec(
            ctx, instance, command=("psql", "--user", "test", "--dbname", "test")
        )
    expected_env = {
        "PGDATA": str(instance.datadir),
        "PGPASSFILE": str(ctx.settings.postgresql.auth.passfile),
        "PGPORT": "999",
        "PGUSER": "postgres",
        "PGHOST": "/socks",
        "PGPASSWORD": "qwerty",
        "PSQLRC": str(instance.psqlrc),
        "PSQL_HISTORY": str(instance.psql_history),
        "PGBACKREST_CONFIG": f"{ctx.settings.prefix}/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf",
        "PGBACKREST_STANZA": f"{instance.version}-{instance.name}",
    }

    bindir = instances.pg_ctl(instance.version, ctx=ctx).bindir
    cmd = [
        f"{bindir}/psql",
        "--user",
        "test",
        "--dbname",
        "test",
    ]
    patched.assert_called_once_with(f"{bindir}/psql", cmd, expected_env)


def test_env(ctx: Context, instance: Instance) -> None:
    bindir = instances.pg_ctl(instance.version, ctx=ctx).bindir
    with patch.dict("os.environ", {"PATH": "/pg10/bin"}):
        expected_env = [
            f"export PATH={bindir}:/pg10/bin",
            f"export PGBACKREST_CONFIG={ctx.settings.prefix}/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf",
            f"export PGBACKREST_STANZA={instance.version}-{instance.name}",
            f"export PGDATA={instance.datadir}",
            "export PGHOST=/socks",
            f"export PGPASSFILE={ctx.settings.postgresql.auth.passfile}",
            "export PGPORT=999",
            "export PGUSER=postgres",
            f"export PSQLRC={instance.psqlrc}",
            f"export PSQL_HISTORY={instance.psql_history}",
        ]
        assert instances.env(ctx, instance) == "\n".join(expected_env)


def test_exists(ctx: Context, instance: Instance) -> None:
    assert instances.exists(ctx, instance.name, instance.version)
    assert not instances.exists(ctx, "doesnotexists", instance.version)


def test_upgrade_forbid_same_instance(ctx: Context, instance: Instance) -> None:
    with pytest.raises(
        exceptions.InvalidVersion,
        match=f"Could not upgrade {instance.version}/test using same name and same version",
    ):
        instances.upgrade(ctx, instance, version=instance.version)


def test_upgrade_target_instance_exists(ctx: Context, instance: Instance) -> None:
    orig_instance = attr.evolve(instance, name="old")
    with pytest.raises(exceptions.InstanceAlreadyExists):
        instances.upgrade(
            ctx, orig_instance, version=instance.version, name=instance.name
        )


def test_upgrade_confirm(ctx: Context, instance: Instance, pg_version: str) -> None:
    with patch.object(ctx, "confirm", return_value=False) as confirm:
        with pytest.raises(exceptions.Cancelled):
            instances.upgrade(ctx, instance, name="new")
    confirm.assert_called_once_with(
        f"Confirm upgrade of instance {instance} to version {pg_version}?",
        True,
    )


def test_standby_upgrade(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby instance$",
    ):
        instances.upgrade(
            ctx, standby_instance, version=str(int(standby_instance.version) + 1)
        )


def test_non_standby_promote(ctx: Context, instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceStateError,
        match=f"^{instance.version}/test is not a standby$",
    ):
        instances.promote(ctx, instance)


def test_logs(ctx: Context, instance: Instance, tmp_path: pathlib.Path) -> None:
    with pytest.raises(
        exceptions.FileNotFoundError,
        match=r"file 'current_logfiles' for instance \d{2}/test not found",
    ):
        next(instances.logs(ctx, instance))

    current_logfiles = instance.datadir / "current_logfiles"
    current_logfiles.write_text("csvlog log/postgresql.csv\n")
    with pytest.raises(ValueError, match="no record matching 'stderr'"):
        next(instances.logs(ctx, instance))

    stderr_logpath = tmp_path / "postgresql.log"
    current_logfiles.write_text(f"stderr {stderr_logpath}\n")
    with pytest.raises(exceptions.SystemError, match="failed to read"):
        next(instances.logs(ctx, instance))

    stderr_logpath.write_text("line1\nline2\n")
    assert list(instances.logs(ctx, instance)) == ["line1\n", "line2\n"]


def test_check_pending_actions(
    ctx: Context,
    instance: Instance,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _settings = [
        interface.PGSetting(
            name="needs_restart",
            context="postmaster",
            setting="somevalue",
            pending_restart=False,
        ),
        interface.PGSetting(
            name="needs_reload",
            context="sighup",
            setting="somevalue",
            pending_restart=False,
        ),
    ]
    changes: ConfigChanges = {
        "needs_restart": ("before", "after"),
        "needs_reload": ("before", "after"),
    }

    restart_on_changes = True
    with patch.object(
        instances, "status", return_value=instances.Status.running
    ), patch.object(
        instances, "settings", return_value=_settings
    ) as settings, patch.object(
        instances, "reload"
    ) as reload, patch.object(
        ctx, "confirm", return_value=False
    ) as confirm, caplog.at_level(
        logging.INFO
    ):
        instances.check_pending_actions(ctx, instance, changes, restart_on_changes)
    confirm.assert_called_once_with(
        "Instance needs to be restarted; restart now?", restart_on_changes
    )
    settings.assert_called_once()
    assert (
        f"instance {instance} needs restart due to parameter changes: needs_restart"
        in caplog.messages
    )
    assert (
        f"instance {instance} needs reload due to parameter changes: needs_reload"
        in caplog.messages
    )
    reload.assert_called_once_with(ctx, instance)


def test_replication_lag(
    ctx: Context, instance: Instance, standby_instance: Instance
) -> None:
    with pytest.raises(TypeError, match="not a standby"):
        instances.replication_lag(ctx, instance)
