import subprocess

import psycopg2
import pytest
from pgtoolkit.conf import parse as parse_pgconf
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

    # Lookup failed.
    pg_version = i.datadir / "PG_VERSION"
    pg_version.write_text("7.1")
    with pytest.raises(
        Exception,
        match="version mismatch",
    ):
        with task.runner():
            instance_mod.init(ctx, i)
    assert not pg_version.exists()  # per revert

    # A failed init cleans up postgres directories.
    pgroot = ctx.settings.postgresql.root / "pg"
    ctx1 = Context(
        plugin_manager=ctx.pm,
        settings=ctx.settings.copy(
            update={"postgresql": PostgreSQLSettings(root=pgroot)}
        ),
    )
    pgroot.mkdir()
    i = Instance.default_version("test", ctx=ctx1)
    i.datadir.mkdir(parents=True)
    (i.datadir / "dirty").touch()
    with pytest.raises(subprocess.CalledProcessError):
        with task.runner():
            instance_mod.init(ctx1, i)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_enabled(ctx, instance_mod.systemd_unit(i))

    # Init failed. Version doesn't match installed one.
    i = Instance("test", "9.6", settings=ctx.settings)
    with pytest.raises(EnvironmentError, match="pg_ctl executable not found"):
        instance_mod.init(ctx, i)


@pytest.fixture
def ctx_nohook(ctx):
    unregistered = ctx.pm.unregister_all()
    try:
        yield ctx
    finally:
        for plugin in unregistered:
            ctx.pm.register(plugin)


def test_configure(ctx_nohook):
    ctx = ctx_nohook
    i = Instance.default_version("test-configure", ctx=ctx)
    configdir = i.datadir
    configdir.mkdir(parents=True)
    postgresql_conf = i.datadir / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour_name = 'test'\n")
    initial_content = postgresql_conf.read_text()

    changes = instance_mod.configure(ctx, i, port=5433, max_connections=100)
    assert changes == {
        "cluster_name": (None, "test-configure"),
        "max_connections": (None, 100),
        "port": (None, 5433),
    }
    with postgresql_conf.open() as f:
        line1 = f.readline().strip()
    assert line1 == "include_dir = 'conf.pglift.d'"

    configfpath = configdir / "conf.pglift.d" / "user.conf"
    lines = configfpath.read_text().splitlines()
    assert "port = 5433" in lines
    assert "cluster_name = 'test-configure'" in lines

    with postgresql_conf.open() as f:
        config = parse_pgconf(f)
    assert config.port == 5433
    assert config.bonjour_name == "test"
    assert config.cluster_name == "test-configure"

    changes = instance_mod.configure(ctx, i, listen_address="*", ssl=True)
    assert changes == {
        "listen_address": (None, "*"),
        "max_connections": (100, None),
        "port": (5433, None),
        "ssl": (None, True),
    }
    # Same configuration, no change.
    mtime_before = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    changes = instance_mod.configure(ctx, i, listen_address="*", ssl=True)
    assert changes == {}
    mtime_after = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    assert mtime_before == mtime_after

    instance_mod.revert_configure(ctx, i)
    assert postgresql_conf.read_text() == initial_content
    assert not configfpath.exists()

    instance_mod.configure(ctx, i, ssl=True)
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert (configdir / "server.crt").exists()
    assert (configdir / "server.key").exists()

    instance_mod.revert_configure(ctx, i, ssl=True)
    assert not (configdir / "server.crt").exists()
    assert not (configdir / "server.key").exists()

    ssl = (cert_file, key_file) = (i.datadir / "c.crt", i.datadir / "k.key")
    for fpath in ssl:
        fpath.touch()
    changes = instance_mod.configure(ctx, i, ssl=ssl)
    assert changes == {
        "cluster_name": (None, i.name),
        "ssl": (None, True),
        "ssl_cert_file": (None, cert_file),
        "ssl_key_file": (None, key_file),
    }
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert f"ssl_cert_file = {i.datadir / 'c.crt'}" in lines
    assert f"ssl_key_file = {i.datadir / 'k.key'}" in lines
    instance_mod.revert_configure(ctx, i, ssl=ssl)
    for fpath in ssl:
        assert fpath.exists()


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
    instance_mod.configure_auth(ctx, i)
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

    instance_mod.revert_configure_auth(ctx, i)

    if passfile:
        assert not passfile.exists()


def test_start_stop(ctx, instance, tmp_path):
    i = instance
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_active(ctx, instance_mod.systemd_unit(i))

    instance_mod.start(ctx, i)
    try:
        assert instance_mod.status(ctx, i) == Status.running
        if ctx.settings.service_manager == "systemd":
            assert systemd.is_active(ctx, instance_mod.systemd_unit(i))
    finally:
        instance_mod.stop(ctx, i)
    assert instance_mod.status(ctx, i) == Status.not_running
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_active(ctx, instance_mod.systemd_unit(i))

    instance_mod.start(ctx, i, logfile=tmp_path / "log")
    try:
        assert instance_mod.status(ctx, i) == Status.running
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
    i = Instance.default_version("test", ctx=ctx)
    instance_mod.init(ctx, i)
    instance_mod.drop(ctx, i)
    assert not i.exists()
