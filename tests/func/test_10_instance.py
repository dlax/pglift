from pathlib import Path

import psycopg2
import pytest
from pgtoolkit.ctl import Status

from pglift import exceptions
from pglift import instance as instance_mod
from pglift import manifest, systemd

from . import reconfigure_instance


def test_init(ctx, instance_initialized, monkeypatch):
    i = instance_initialized
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

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, instance_mod.systemd_unit(i))

    # Instance alread exists, no-op.
    with monkeypatch.context() as m:

        def fail():
            raise AssertionError("unexpected called")

        m.setattr(ctx, "pg_ctl", fail)
        instance_mod.init(ctx, i)


def test_log_directory(ctx, instance, log_directory):
    config = instance.config()
    instance_log_dir = Path(config.log_directory)
    assert instance_log_dir == log_directory
    assert instance_log_dir.exists()


def test_pgpass(ctx, instance):
    port = instance.port
    passfile = ctx.settings.postgresql.auth.passfile
    if ctx.settings.postgresql.surole.pgpass:
        assert passfile.read_text().splitlines()[1:] == [f"*:{port}:*:postgres:s3kret"]

        with reconfigure_instance(ctx, instance, port=port + 1):
            assert passfile.read_text().splitlines() == [
                "#hostname:port:database:username:password",
                f"*:{port+1}:*:postgres:s3kret",
            ]

        assert passfile.read_text().splitlines()[1:] == [f"*:{port}:*:postgres:s3kret"]


def test_auth(ctx, instance):
    i = instance
    surole = ctx.settings.postgresql.surole
    port = i.port
    connargs = {
        "host": str(i.config().unix_socket_directories),
        "port": port,
        "user": surole.name,
    }

    passfile = ctx.settings.postgresql.auth.passfile

    password = None
    if surole.password:
        password = surole.password.get_secret_value()

    with instance_mod.running(ctx, i):
        if password is not None:
            with pytest.raises(psycopg2.OperationalError, match="no password supplied"):
                psycopg2.connect(**connargs).close()
            if surole.pgpass:
                connargs["passfile"] = str(passfile)
            else:
                connargs["password"] = password
        psycopg2.connect(**connargs).close()

    hba_path = i.datadir / "pg_hba.conf"
    hba = hba_path.read_text().splitlines()
    auth = ctx.settings.postgresql.auth
    assert (
        f"local   all             all                                     {auth.local}"
        in hba
    )
    assert (
        f"host    all             all             127.0.0.1/32            {auth.host}"
        in hba
    )

    ident_path = i.datadir / "pg_ident.conf"
    ident = ident_path.read_text().splitlines()
    assert ident == ["# MAPNAME       SYSTEM-USERNAME         PG-USERNAME"]


def test_start_stop(ctx, instance, tmp_path):
    i = instance
    use_systemd = ctx.settings.service_manager == "systemd"
    if use_systemd:
        assert not systemd.is_active(ctx, instance_mod.systemd_unit(i))

    instance_mod.start(ctx, i)
    try:
        assert instance_mod.status(ctx, i) == Status.running
        if use_systemd:
            assert systemd.is_active(ctx, instance_mod.systemd_unit(i))
    finally:
        instance_mod.stop(ctx, i)
    assert instance_mod.status(ctx, i) == Status.not_running
    if use_systemd:
        assert not systemd.is_active(ctx, instance_mod.systemd_unit(i))

    instance_mod.start(ctx, i, logfile=tmp_path / "log")
    try:
        assert instance_mod.status(ctx, i) == Status.running
        if not use_systemd:
            # FIXME: systemctl restart would fail with:
            #   Start request repeated too quickly.
            #   Failed with result 'start-limit-hit'.
            instance_mod.restart(ctx, i)
            assert instance_mod.status(ctx, i) == Status.running
        instance_mod.reload(ctx, i)
        assert instance_mod.status(ctx, i) == Status.running
    finally:
        instance_mod.stop(ctx, i, mode="immediate")
    assert instance_mod.status(ctx, i) == Status.not_running


def test_apply(ctx, installed, tmp_path, tmp_port_factory):
    port = next(tmp_port_factory)
    prometheus_port = next(tmp_port_factory)
    im = manifest.Instance(
        name="test_apply",
        port=port,
        ssl=True,
        state=manifest.InstanceState.stopped,
        configuration={"unix_socket_directories": str(tmp_path)},
        prometheus={"port": prometheus_port},
    )
    i = instance_mod.apply(ctx, im)
    assert i is not None
    assert i.exists()
    assert i.port == port
    pgconfig = i.config()
    assert pgconfig
    assert pgconfig.ssl

    assert instance_mod.status(ctx, i) == Status.not_running
    im.state = manifest.InstanceState.started
    instance_mod.apply(ctx, im)
    assert instance_mod.status(ctx, i) == Status.running

    im.configuration["bonjour"] = False
    instance_mod.apply(ctx, im)
    assert instance_mod.status(ctx, i) == Status.running

    im.state = manifest.InstanceState.stopped
    instance_mod.apply(ctx, im)
    assert instance_mod.status(ctx, i) == Status.not_running

    im.state = manifest.InstanceState.absent
    instance_mod.apply(ctx, im)
    with pytest.raises(exceptions.InstanceNotFound):
        i.exists()
    assert not i.as_spec().exists()
    assert instance_mod.status(ctx, i) == Status.unspecified_datadir


def test_describe(ctx, instance, log_directory):
    i = instance
    im = instance_mod.describe(ctx, i)
    assert im is not None
    assert im.name == "test"
    config = im.configuration
    assert im.port == i.port
    config.pop("unix_socket_directories")
    if "log_directory" in config:
        assert config.pop("log_directory") == str(log_directory)
    assert config == {}
    assert im.state.name == "stopped"


def test_list(ctx, instance):
    (ctx.settings.postgresql.root / "12/notAnInstanceDir").mkdir(parents=True)
    (i,) = list(instance_mod.list(ctx))
    assert i.name == instance.name
    assert i.version == instance.version
    assert i.status == Status.not_running.name

    with pytest.raises(ValueError, match="unknown version '7'"):
        next(instance_mod.list(ctx, version="7"))

    iv = next(instance_mod.list(ctx, version=instance.version))
    assert iv == i

    other_version = next(
        v for v in ctx.settings.postgresql.versions if v != instance.version
    )
    assert list(instance_mod.list(ctx, version=other_version)) == []
