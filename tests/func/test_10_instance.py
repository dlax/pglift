import subprocess

import psycopg2
import pytest
from pgtoolkit.ctl import Status

from pglift import instance as instance_mod
from pglift import manifest, systemd, task
from pglift.ctx import Context
from pglift.model import Instance
from pglift.settings import PostgreSQLSettings


def test_init(ctx, instance_initialized):
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
    ret = instance_mod.init(ctx, i)
    assert not ret


def test_init_lookup_failed(ctx):
    i = Instance.default_version("dirty", ctx=ctx)
    i.datadir.mkdir(parents=True)
    (i.datadir / "postgresql.conf").touch()
    pg_version = i.datadir / "PG_VERSION"
    pg_version.write_text("7.1")
    with pytest.raises(
        Exception,
        match="version mismatch",
    ):
        with task.runner():
            instance_mod.init(ctx, i)
    assert not pg_version.exists()  # per revert


def test_init_dirty(ctx):
    pgroot = ctx.settings.postgresql.root / "pg"
    ctx1 = Context(
        plugin_manager=ctx.pm,
        settings=ctx.settings.copy(
            update={"postgresql": PostgreSQLSettings(root=pgroot)}
        ),
    )
    pgroot.mkdir()
    i = Instance.default_version("dirty", ctx=ctx1)
    i.datadir.mkdir(parents=True)
    (i.datadir / "dirty").touch()
    with pytest.raises(subprocess.CalledProcessError):
        with task.runner():
            instance_mod.init(ctx1, i)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_enabled(ctx, instance_mod.systemd_unit(i))


def test_init_version_not_available(ctx):
    i = Instance("pg96", "9.6", settings=ctx.settings)
    with pytest.raises(EnvironmentError, match="pg_ctl executable not found"):
        instance_mod.init(ctx, i)


def test_configure_auth(ctx, instance_auth_configured):
    i = instance_auth_configured
    surole = ctx.settings.postgresql.surole
    connargs = {
        "host": str(i.config().unix_socket_directories),
        "port": i.config().port,
        "user": surole.name,
    }

    passfile = None
    if ctx.settings.postgresql.surole.pgpass:
        passfile = ctx.settings.postgresql.auth.passfile

    if passfile:
        connargs["passfile"] = str(passfile)

    password = surole.password.get_secret_value()
    with instance_mod.running(ctx, i):
        if not passfile:
            with pytest.raises(psycopg2.OperationalError, match="no password supplied"):
                psycopg2.connect(**connargs).close()
        psycopg2.connect(password=password, **connargs).close()

    hba_path = i.datadir / "pg_hba.conf"
    hba = hba_path.read_text().splitlines()
    assert (
        "local   all             all                                     password"
        in hba
    )
    assert (
        "host    all             all             127.0.0.1/32            reject" in hba
    )

    if passfile:
        assert surole.name in passfile.read_text()


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


def test_apply(ctx, installed, tmp_path, tmp_port):
    im = manifest.Instance(
        name="test_apply",
        ssl=True,
        state=manifest.InstanceState.stopped,
        configuration={"unix_socket_directories": str(tmp_path), "port": tmp_port},
    )
    i = im.model(ctx)
    instance_mod.apply(ctx, im)
    assert i.exists()
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
    assert not i.exists()
    assert instance_mod.status(ctx, i) == Status.unspecified_datadir


def test_describe_absent(ctx, installed):
    i = Instance("absent", "9.6")
    im = instance_mod.describe(ctx, i)
    assert im is None


def test_describe(ctx, instance):
    i = instance
    im = instance_mod.describe(ctx, i)
    assert im is not None
    assert im.name == "test"
    config = im.configuration
    config.pop("port")
    config.pop("unix_socket_directories")
    assert config == {"cluster_name": "test"}
    assert im.state.name == "stopped"


def test_drop_absent(ctx, installed):
    i = Instance("absent", "9.6")
    instance_mod.drop(ctx, i)


def test_drop(ctx, installed):
    i = Instance.default_version("dropme", ctx=ctx)
    instance_mod.init(ctx, i)
    instance_mod.drop(ctx, i)
    assert not i.exists()
