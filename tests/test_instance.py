import subprocess

import psycopg2
import pytest
from pgtoolkit.conf import parse as parse_pgconf
from pgtoolkit.ctl import Status

from pglift import instance, manifest, systemd, task
from pglift.ctx import Context
from pglift.model import Instance
from pglift.settings import InitdbSettings, PostgreSQLSettings

from . import instance_running


@pytest.fixture
def ctx(ctx):
    ctx.pm.unregister_all()
    return ctx


def test_init(ctx, installed):
    i = Instance.default_version("test", ctx=ctx)
    ret = instance.init(ctx, i)
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

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, instance.systemd_unit(i))

    # Instance alread exists, no-op.
    ret = instance.init(ctx, i)
    assert not ret

    # Lookup failed.
    pg_version = i.datadir / "PG_VERSION"
    pg_version.write_text("7.1")
    with pytest.raises(
        Exception,
        match="version mismatch",
    ):
        with task.runner():
            instance.init(ctx, i)
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
            instance.init(ctx1, i)
    assert not i.datadir.exists()  # XXX: not sure this is a sane thing to do?
    assert not i.waldir.exists()
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_enabled(ctx, instance.systemd_unit(i))

    # Init failed. Version doesn't match installed one.
    i = Instance("test", "9.6", settings=ctx.settings)
    with pytest.raises(EnvironmentError, match="pg_ctl executable not found"):
        instance.init(ctx, i)


def test_init_surole_pwprompt(ctx, tmp_path, installed, monkeypatch):
    calls = []

    def cmd_run(args, **kwargs):
        calls.append(args)

    pgroot = ctx.settings.postgresql.root
    ctx1 = Context(
        plugin_manager=ctx.pm,
        settings=ctx.settings.copy(
            update={
                "postgresql": PostgreSQLSettings(
                    root=pgroot, initdb=InitdbSettings(auth=("md5", None))
                )
            }
        ),
    )
    i = Instance.default_version("test", ctx=ctx1)
    ctx1.pg_ctl(i.version)  # warm cache to avoid mock side effect
    with monkeypatch.context() as m:
        m.setattr("pglift.cmd.run", cmd_run)
        instance.init(ctx1, i)

    init_cmd = " ".join(calls[0])
    assert "--auth=md5" in init_cmd
    assert "--pwprompt" in init_cmd


def test_init_surole_pwfile(ctx, tmp_path, installed, tmp_port):
    pgroot = ctx.settings.postgresql.root
    surole_pwd = "S3kret"
    pwfile = tmp_path / "surole_pwd"
    with open(pwfile, "w") as f:
        f.write(surole_pwd)
    ctx1 = Context(
        plugin_manager=ctx.pm,
        settings=ctx.settings.copy(
            update={
                "postgresql": PostgreSQLSettings(
                    root=pgroot, initdb=InitdbSettings(auth=("md5", pwfile))
                )
            }
        ),
    )
    i = Instance.default_version("test", ctx=ctx1)
    instance.init(ctx1, i)
    instance.configure(ctx1, i, unix_socket_directories=str(tmp_path), port=tmp_port)

    with instance_running(ctx1, i):
        connargs = {
            "user": ctx1.settings.postgresql.surole,
            "host": str(tmp_path),
            "port": tmp_port,
        }
        with pytest.raises(psycopg2.OperationalError, match="no password supplied"):
            psycopg2.connect(**connargs)

        connargs["password"] = surole_pwd
        psycopg2.connect(**connargs)


def test_configure(ctx):
    i = Instance.default_version("test", ctx=ctx)
    configdir = i.datadir
    configdir.mkdir(parents=True)
    postgresql_conf = i.datadir / "postgresql.conf"
    with postgresql_conf.open("w") as f:
        f.write("bonjour = 'test'\n")
    initial_content = postgresql_conf.read_text()

    changes = instance.configure(ctx, i, port=5433, max_connections=100)
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
    assert config.bonjour == "test"
    assert config.cluster_name == "test"

    changes = instance.configure(ctx, i, listen_address="*", ssl=True)
    assert changes == {
        "listen_address": (None, "*"),
        "max_connections": (100, None),
        "port": (5433, None),
        "ssl": (None, True),
    }
    # Same configuration, no change.
    mtime_before = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    changes = instance.configure(ctx, i, listen_address="*", ssl=True)
    assert changes == {}
    mtime_after = postgresql_conf.stat().st_mtime, configfpath.stat().st_mtime
    assert mtime_before == mtime_after

    instance.revert_configure(ctx, i)
    assert postgresql_conf.read_text() == initial_content
    assert not configfpath.exists()

    instance.configure(ctx, i, ssl=True)
    lines = configfpath.read_text().splitlines()
    assert "ssl = on" in lines
    assert (configdir / "server.crt").exists()
    assert (configdir / "server.key").exists()

    instance.revert_configure(ctx, i, ssl=True)
    assert not (configdir / "server.crt").exists()
    assert not (configdir / "server.key").exists()

    ssl = (cert_file, key_file) = (i.datadir / "c.crt", i.datadir / "k.key")
    for fpath in ssl:
        fpath.touch()
    changes = instance.configure(ctx, i, ssl=ssl)
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
    instance.revert_configure(ctx, i, ssl=ssl)
    for fpath in ssl:
        assert fpath.exists()


def test_start_stop(ctx, installed, tmp_path, tmp_port):
    i = Instance.default_version("test", ctx=ctx)
    assert instance.status(ctx, i) == Status.unspecified_datadir

    instance.init(ctx, i)
    instance.configure(
        ctx,
        i,
        port=tmp_port,
        log_destination="syslog",
        unix_socket_directories=str(tmp_path),
    )
    assert instance.status(ctx, i) == Status.not_running
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_active(ctx, instance.systemd_unit(i))

    instance.start(ctx, i)
    try:
        assert instance.status(ctx, i) == Status.running
        if ctx.settings.service_manager == "systemd":
            assert systemd.is_active(ctx, instance.systemd_unit(i))
    finally:
        instance.stop(ctx, i)
    assert instance.status(ctx, i) == Status.not_running
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_active(ctx, instance.systemd_unit(i))

    instance.start(ctx, i, logfile=tmp_path / "log")
    try:
        assert instance.status(ctx, i) == Status.running
        instance.restart(ctx, i)
        assert instance.status(ctx, i) == Status.running
        instance.reload(ctx, i)
        assert instance.status(ctx, i) == Status.running
    finally:
        instance.stop(ctx, i, mode="immediate")
    assert instance.status(ctx, i) == Status.not_running


def test_apply(ctx, installed, tmp_path):
    im = manifest.Instance(name="test", ssl=True, state=manifest.InstanceState.stopped)
    i = im.model(ctx)
    instance.apply(ctx, im)
    assert i.exists()
    pgconfig = i.config()
    assert pgconfig
    assert pgconfig.ssl

    im.state = manifest.InstanceState.started
    im.configuration["unix_socket_directories"] = str(tmp_path)
    instance.apply(ctx, im)
    assert instance.status(ctx, i) == Status.running

    im.configuration["bonjour"] = False
    instance.apply(ctx, im)
    assert instance.status(ctx, i) == Status.running

    im.state = manifest.InstanceState.stopped
    instance.apply(ctx, im)
    assert instance.status(ctx, i) == Status.not_running

    im.state = manifest.InstanceState.absent
    instance.apply(ctx, im)
    assert not i.exists()
    assert instance.status(ctx, i) == Status.unspecified_datadir


def test_describe(ctx, installed):
    i = Instance("absent", "9.6")
    im = instance.describe(ctx, i)
    assert im is None

    i = Instance.default_version("test", ctx=ctx)
    instance.init(ctx, i)
    instance.configure(ctx, i, shared_buffers="10MB")
    im = instance.describe(ctx, i)
    assert im is not None
    assert im.name == "test"
    assert im.configuration == {"cluster_name": "test", "shared_buffers": "10MB"}
    assert im.state.name == "stopped"


def test_drop(ctx, installed):
    i = Instance("absent", "9.6")
    instance.drop(ctx, i)

    i = Instance.default_version("test", ctx=ctx)
    instance.init(ctx, i)
    instance.drop(ctx, i)
    assert not i.exists()
