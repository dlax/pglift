import logging
import re
from pathlib import Path
from typing import Iterator, List, NoReturn, Optional, Tuple, Type
from unittest.mock import patch

import psycopg
import pytest
from pgtoolkit.ctl import Status
from pydantic import SecretStr
from tenacity import retry
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from pglift import databases, exceptions, instances, systemd
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.settings import Settings

from . import AuthType, execute, reconfigure_instance
from .conftest import DatabaseFactory


def test_directories(instance: system.Instance) -> None:
    assert instance.datadir.exists()
    assert instance.waldir.exists()
    assert (instance.waldir / "archive_status").is_dir()


def test_config(instance: system.Instance) -> None:
    postgresql_conf = instance.datadir / "postgresql.conf"
    assert postgresql_conf.exists()
    with postgresql_conf.open() as f:
        for line in f:
            if line.strip() == "include_dir = 'conf.pglift.d'":
                continue
            sline = line.lstrip()
            assert not sline or sline.startswith(
                "#"
            ), f"found uncommented line in postgresql.conf: {line}"


def test_psqlrc(instance: system.Instance) -> None:
    assert instance.psqlrc.read_text().strip().splitlines() == [
        f"\\set PROMPT1 '[{instance}] %n@%~%R%x%# '",
        "\\set PROMPT2 ' %R%x%# '",
    ]


def test_systemd(ctx: Context, instance: system.Instance) -> None:
    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, instances.systemd_unit(instance))


def test_reinit(
    ctx: Context,
    instance: system.PostgreSQLInstance,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Instance already exists, no-op.
    with monkeypatch.context() as m:

        def fail() -> NoReturn:
            raise AssertionError("unexpected called")

        m.setattr(instances, "pg_ctl", fail)
        instances.init(
            ctx, interface.Instance(name=instance.name, version=instance.version)
        )


def test_log_directory(instance: system.Instance, log_directory: Path) -> None:
    config = instance.config()
    assert isinstance(config.log_directory, str)
    instance_log_dir = Path(config.log_directory)
    assert instance_log_dir == log_directory
    assert instance_log_dir.exists()


def passfile_entries(passfile: Path, *, role: str = "postgres") -> List[str]:
    return [line for line in passfile.read_text().splitlines() if f":{role}:" in line]


def test_pgpass(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    postgresql_auth: AuthType,
) -> None:
    port = instance.port
    passfile = ctx.settings.postgresql.auth.passfile

    if postgresql_auth == AuthType.pgpass:
        assert passfile_entries(passfile) == [f"*:{port}:*:postgres:s3kret"]

        with reconfigure_instance(ctx, instance_manifest, port=port + 1):
            assert passfile_entries(passfile) == [f"*:{port+1}:*:postgres:s3kret"]

        assert passfile_entries(passfile) == [f"*:{port}:*:postgres:s3kret"]


def test_connect(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    postgresql_auth: AuthType,
) -> None:
    i = instance
    surole = instance_manifest.surole(ctx.settings)
    port = i.port
    connargs = {
        "host": str(i.config().unix_socket_directories),
        "port": port,
        "user": surole.name,
    }
    with instances.running(ctx, i):
        if postgresql_auth == AuthType.peer:
            pass
        elif postgresql_auth == AuthType.pgpass:
            connargs["passfile"] = str(ctx.settings.postgresql.auth.passfile)
        else:
            with pytest.raises(psycopg.OperationalError, match="no password supplied"):
                with patch.dict("os.environ", clear=True):
                    psycopg.connect(**connargs).close()  # type: ignore[call-overload]
            assert surole.password is not None
            connargs["password"] = surole.password.get_secret_value()
        with psycopg.connect(**connargs) as conn:  # type: ignore[call-overload]
            if postgresql_auth == AuthType.peer:
                assert not conn.pgconn.used_password
            else:
                assert conn.pgconn.used_password


def test_hba(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    postgresql_auth: AuthType,
) -> None:
    hba_path = instance.datadir / "pg_hba.conf"
    hba = hba_path.read_text().splitlines()
    auth_settings = ctx.settings.postgresql.auth
    auth_instance = instance_manifest.auth
    assert auth_instance is not None
    if postgresql_auth == AuthType.peer:
        assert "peer" in hba[0]
    assert (
        f"local   all             all                                     {auth_settings.local}"
        in hba
    )
    assert (
        f"host    all             all             127.0.0.1/32            {auth_instance.host}"
        in hba
    )


def test_ident(
    ctx: Context, instance: system.Instance, postgresql_auth: AuthType
) -> None:
    ident_path = instance.datadir / "pg_ident.conf"
    ident = ident_path.read_text().splitlines()
    assert ident[0] == "# MAPNAME       SYSTEM-USERNAME         PG-USERNAME"
    if postgresql_auth == AuthType.peer:
        assert re.match(r"^test\s+\w+\s+postgres$", ident[1])
    else:
        assert len(ident) == 1


def test_start_stop_restart_running_stopped(
    ctx: Context,
    instance: system.Instance,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    i = instance
    use_systemd = ctx.settings.service_manager == "systemd"
    if use_systemd:
        assert not systemd.is_active(ctx, instances.systemd_unit(i))

    instances.start(ctx, i)
    try:
        assert instances.status(ctx, i) == Status.running
        if use_systemd:
            assert systemd.is_active(ctx, instances.systemd_unit(i))
    finally:
        instances.stop(ctx, i)

        # Stopping a non-running instance is a no-op.
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="pglift"):
            instances.stop(ctx, i)
        assert f"instance {instance} is already stopped" in caplog.records[0].message

    assert instances.status(ctx, i) == Status.not_running
    if use_systemd:
        assert not systemd.is_active(ctx, instances.systemd_unit(i))

    instances.start(ctx, i, logfile=tmp_path / "log", run_hooks=False)
    try:
        assert instances.status(ctx, i) == Status.running
        if not use_systemd:
            # FIXME: systemctl restart would fail with:
            #   Start request repeated too quickly.
            #   Failed with result 'start-limit-hit'.
            instances.restart(ctx, i)
            assert instances.status(ctx, i) == Status.running
        instances.reload(ctx, i)
        assert instances.status(ctx, i) == Status.running
    finally:
        instances.stop(ctx, i, mode="immediate", run_hooks=False)

    assert instances.status(ctx, i) == Status.not_running
    with instances.stopped(ctx, i):
        assert instances.status(ctx, i) == Status.not_running
        with instances.stopped(ctx, i):
            assert instances.status(ctx, i) == Status.not_running
        with instances.running(ctx, i):
            assert instances.status(ctx, i) == Status.running
            with instances.running(ctx, i):
                assert instances.status(ctx, i) == Status.running
            with instances.stopped(ctx, i):
                assert instances.status(ctx, i) == Status.not_running
            assert instances.status(ctx, i) == Status.running
        assert instances.status(ctx, i) == Status.not_running
    assert instances.status(ctx, i) == Status.not_running


@pytest.mark.usefixtures("installed")
def test_apply(
    ctx: Context,
    pg_version: str,
    tmp_path: Path,
    tmp_port_factory: Iterator[int],
    surole_password: str,
    composite_instance_model: Type[interface.Instance],
    caplog: pytest.LogCaptureFixture,
) -> None:
    port = next(tmp_port_factory)
    prometheus_port = next(tmp_port_factory)
    temboard_port = next(tmp_port_factory)
    im = composite_instance_model(
        name="test_apply",
        version=pg_version,
        port=port,
        ssl=True,
        state=interface.InstanceState.stopped,
        configuration={"unix_socket_directories": str(tmp_path)},
        prometheus={"port": prometheus_port},
        temboard={"port": temboard_port},
        surole_password=surole_password,
        restart_on_changes=False,
        roles=[{"name": "bob"}],
        databases=[
            {"name": "db1"},
            {"name": "db2", "owner": "bob", "extensions": ["unaccent"]},
        ],
    )
    assert instances.apply(ctx, im)
    i = system.Instance.system_lookup(ctx, ("test_apply", pg_version))
    assert i.exists()
    assert i.port == port
    pgconfig = i.config()
    assert pgconfig
    assert pgconfig.ssl

    assert not instances.apply(ctx, im)  # no-op

    assert instances.status(ctx, i) == Status.not_running
    im = im._copy_validate({"state": "started"})
    assert instances.apply(ctx, im)
    assert instances.status(ctx, i) == Status.running
    assert not instances.pending_restart(ctx, i)

    with instances.running(ctx, i):
        assert databases.exists(ctx, i, "db1")
        assert databases.exists(ctx, i, "db2")
        db = databases.get(ctx, i, "db2")
        assert db.extensions == [interface.Extension.unaccent]
        assert db.owner == "bob"

    newconfig = im.configuration.copy()
    newconfig["listen_addresses"] = "*"  # requires restart
    newconfig["autovacuum"] = False  # requires reload
    im = im._copy_validate({"configuration": newconfig})
    with caplog.at_level(logging.DEBUG, logger="pgflit"):
        assert instances.apply(ctx, im)
    assert (
        f"instance {i} needs restart due to parameter changes: listen_addresses"
        in caplog.messages
    )
    assert instances.status(ctx, i) == Status.running
    assert instances.pending_restart(ctx, i)

    im = im._copy_validate({"state": "stopped"})
    assert instances.apply(ctx, im)
    assert instances.status(ctx, i) == Status.not_running

    im = im._copy_validate({"state": "absent"})
    assert instances.apply(ctx, im) is None
    with pytest.raises(exceptions.InstanceNotFound):
        i.exists()
    assert instances.status(ctx, i) == Status.unspecified_datadir


def test_get(
    ctx: Context, instance: system.Instance, log_directory: Path, powa_available: bool
) -> None:
    im = instances.get(ctx, instance.name, instance.version)
    assert im is not None
    assert im.name == "test"
    config = im.configuration
    assert im.port == instance.port
    if "log_directory" in config:
        logdir = config.pop("log_directory")
        assert logdir == str(log_directory)
    spl = "passwordcheck"
    extensions = ["passwordcheck"]
    if powa_available:
        spl += ", pg_qualstats, pg_stat_statements, pg_stat_kcache"
        extensions += ["pg_qualstats", "pg_stat_statements", "pg_stat_kcache"]
    assert config == {
        "lc_messages": "C",
        "lc_monetary": "C",
        "lc_numeric": "C",
        "lc_time": "C",
        "logging_collector": False,
        "shared_preload_libraries": spl,
    }
    if int(instance.version) <= 10:
        assert im.data_checksums is None
    else:
        assert im.data_checksums is False
    assert im.state.name == "stopped"
    assert not im.surole_password
    assert [e.name for e in im.extensions] == extensions
    assert not im.pending_restart

    with instances.running(ctx, instance):
        im = instances.get(ctx, instance.name, instance.version)
        assert isinstance(im.surole_password, SecretStr)
        assert not im.pending_restart


def test_list(ctx: Context, instance: system.Instance) -> None:
    not_instance_dir = ctx.settings.postgresql.root / "12" / "notAnInstanceDir"
    not_instance_dir.mkdir(parents=True)
    try:
        ilist = list(instances.list(ctx))

        for i in ilist:
            assert i.status == Status.not_running.name
            # this also ensure instance name is not notAnInstanceDir
            assert i.name == "test"

        for i in ilist:
            if (i.version, i.name) == (instance.version, instance.name):
                break
        else:
            assert False, f"Instance {instance.version}/{instance.name} not found"

        iv = next(instances.list(ctx, version=instance.version))
        assert iv == i
    finally:
        not_instance_dir.rmdir()


def test_standby_instance(
    ctx: Context,
    instance: system.Instance,
    postgresql_auth: AuthType,
    replrole_password: str,
    standby_manifest: interface.Instance,
    standby_instance: system.Instance,
) -> None:
    assert standby_manifest.standby
    slotname = standby_manifest.standby.slot
    assert standby_instance.standby
    assert standby_instance.standby.for_
    assert (
        standby_instance.standby.password
        and standby_instance.standby.password.get_secret_value() == replrole_password
    )
    assert standby_instance.standby.slot == slotname
    with instances.running(ctx, instance):
        rows = execute(ctx, instance, "SELECT slot_name FROM pg_replication_slots")
    assert [r["slot_name"] for r in rows] == [slotname]


def test_standby_pgpass(
    ctx: Context,
    postgresql_auth: AuthType,
    standby_instance: system.Instance,
) -> None:
    if postgresql_auth != AuthType.pgpass:
        pytest.skip(f"not applicable for auth: {postgresql_auth}")
    passfile = ctx.settings.postgresql.auth.passfile
    port = standby_instance.port
    assert f"*:{port}:*:postgres:s3kret" in passfile_entries(passfile)


def test_standby_replication(
    ctx: Context,
    instance: system.Instance,
    instance_manifest: interface.Instance,
    settings: Settings,
    postgresql_auth: AuthType,
    surole_password: str,
    tmp_port_factory: Iterator[int],
    tmp_path_factory: pytest.TempPathFactory,
    database_factory: DatabaseFactory,
    composite_instance_model: Type[interface.Instance],
    pg_version: str,
    standby_instance: system.Instance,
) -> None:
    assert standby_instance.standby

    surole = instance_manifest.surole(settings)
    replrole = instance_manifest.replrole(settings)

    if surole.password:

        def get_stdby() -> Optional[interface.Instance.Standby]:
            assert surole.password
            with patch.dict(
                "os.environ", {"PGPASSWORD": surole.password.get_secret_value()}
            ):
                return instances._get(ctx, standby_instance).standby

    else:

        def get_stdby() -> Optional[interface.Instance.Standby]:
            return instances._get(ctx, standby_instance).standby

    class OutOfSync(AssertionError):
        pass

    @retry(
        retry=retry_if_exception_type(psycopg.OperationalError),
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
    )
    def assert_db_replicated() -> int:
        row = execute(
            ctx, standby_instance, "SELECT * FROM t", role=replrole, dbname="test"
        )
        return row[0]["i"]  # type: ignore[no-any-return]

    @retry(
        retry=retry_if_exception_type(OutOfSync),
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
    )
    def assert_replicated(expected: int) -> None:
        rlag = instances.replication_lag(ctx, standby_instance)
        assert rlag is not None
        row = execute(
            ctx, standby_instance, "SELECT * FROM t", role=replrole, dbname="test"
        )
        if row[0]["i"] != expected:
            assert rlag > 0
            raise OutOfSync
        if rlag > 0:
            raise OutOfSync
        assert rlag == 0

    with instances.running(ctx, instance), instances.running(ctx, standby_instance):
        database_factory("test")
        execute(
            ctx,
            instance,
            "CREATE TABLE t AS (SELECT 1 AS i)",
            dbname="test",
            fetch=False,
            role=replrole,
        )
        stdby = get_stdby()
        assert stdby is not None
        assert stdby.for_ == standby_instance.standby.for_
        assert stdby.password == replrole.password
        assert stdby.slot == standby_instance.standby.slot
        assert stdby.replication_lag is not None

        assert execute(
            ctx,
            standby_instance,
            "SELECT * FROM pg_is_in_recovery()",
            role=replrole,
            dbname="template1",
        ) == [{"pg_is_in_recovery": True}]

        assert_db_replicated() == 1

        execute(
            ctx,
            instance,
            "UPDATE t SET i = 42",
            dbname="test",
            role=replrole,
            fetch=False,
        )

        assert_replicated(42)

        stdby = get_stdby()
        assert stdby is not None
        assert stdby.replication_lag == 0

        instances.promote(ctx, standby_instance)
        assert not standby_instance.standby
        assert execute(
            ctx,
            standby_instance,
            "SELECT * FROM pg_is_in_recovery()",
            role=replrole,
            dbname="template1",
        ) == [{"pg_is_in_recovery": False}]
        # Check that we can connect to the promoted instance.
        connargs = {
            "host": str(standby_instance.config().unix_socket_directories),
            "port": standby_instance.port,
            "user": surole.name,
        }
        if postgresql_auth == AuthType.peer:
            pass
        elif postgresql_auth == AuthType.pgpass:
            connargs["passfile"] = str(ctx.settings.postgresql.auth.passfile)
        else:
            connargs["password"] = surole_password
        with psycopg.connect(**connargs) as conn:  # type: ignore[call-overload]
            if postgresql_auth == AuthType.peer:
                assert not conn.pgconn.used_password
            else:
                assert conn.pgconn.used_password


def test_instance_upgrade(
    ctx: Context,
    instance: system.Instance,
    tmp_port_factory: Iterator[int],
    database_factory: DatabaseFactory,
) -> None:
    database_factory("present")
    port = next(tmp_port_factory)
    newinstance = instances.upgrade(
        ctx,
        instance,
        name="test_upgrade",
        version=instance.version,
        port=port,
    )
    try:
        assert newinstance.name == "test_upgrade"
        assert newinstance.version == instance.version
        assert newinstance.port == port
        assert instances.status(ctx, newinstance) == Status.not_running
        with instances.running(ctx, newinstance):
            assert databases.exists(ctx, newinstance, "present")
    finally:
        instances.drop(ctx, newinstance)


def test_server_settings(ctx: Context, instance: system.Instance) -> None:
    with instances.running(ctx, instance):
        pgsettings = instances.settings(ctx, instance)
    port = next(p for p in pgsettings if p.name == "port")
    assert port.setting == str(instance.port)
    assert not port.pending_restart
    assert port.context == "postmaster"


def test_logs(
    ctx: Context, instance_manifest: interface.Instance, instance: system.Instance
) -> None:
    with reconfigure_instance(ctx, instance_manifest, logging_collector=True):
        with instances.running(ctx, instance):
            pass
        logs = list(instances.logs(ctx, instance))
        assert "database system is shut down" in logs[-1]


def test_get_locale(ctx: Context, instance: system.Instance) -> None:
    with instances.running(ctx, instance):
        assert instances.get_locale(ctx, instance) == "C"
    postgres_conf = instance.datadir / "postgresql.conf"
    original_conf = postgres_conf.read_text()
    with postgres_conf.open("a") as f:
        f.write("\nlc_numeric = ''\n")
    try:
        with instances.running(ctx, instance):
            assert instances.get_locale(ctx, instance) is None
    finally:
        postgres_conf.write_text(original_conf)


def test_get_encoding(ctx: Context, instance: system.Instance) -> None:
    with instances.running(ctx, instance):
        assert instances.get_encoding(ctx, instance) == "UTF8"


@pytest.fixture
def datachecksums_instance(
    ctx: Context,
    composite_instance_model: Type[interface.Instance],
    pg_version: str,
    tmp_port_factory: Iterator[int],
    surole_password: str,
) -> Iterator[Tuple[interface.Instance, system.Instance]]:
    manifest = composite_instance_model.parse_obj(
        {
            "name": "datachecksums",
            "version": pg_version,
            "port": next(tmp_port_factory),
            "state": "stopped",
            "surole_password": surole_password,
        }
    )
    instances.apply(ctx, manifest)
    instance = system.Instance.system_lookup(ctx, ("datachecksums", pg_version))
    yield manifest, instance
    instances.drop(ctx, instance)


def test_data_checksums(
    ctx: Context,
    pg_version: str,
    datachecksums_instance: Tuple[interface.Instance, system.Instance],
    caplog: pytest.LogCaptureFixture,
) -> None:
    manifest, instance = datachecksums_instance

    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "off"}]

    # explicitly enabled
    manifest = manifest._copy_validate({"data_checksums": True})
    if int(pg_version) < 12:
        with pytest.raises(
            exceptions.UnsupportedError,
            match={
                "10": r"^PostgreSQL <= 10 doesn't allow to offline check for data-checksums$",
                "11": r"^PostgreSQL <= 11 doesn't have pg_checksums to enable data checksums$",
            }[pg_version],
        ):
            instances.apply(ctx, manifest)
        return

    with caplog.at_level(logging.INFO, logger="pglift.instances"):
        assert instances.apply(ctx, manifest)
    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "on"}]
    assert "enabling data checksums" in caplog.messages
    caplog.clear()

    assert instances._get(ctx, instance).data_checksums

    # not explicitly disabled so still enabled
    assert (
        instances.apply(ctx, manifest._copy_validate({"data_checksums": None})) is False
    )
    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "on"}]

    # explicitly disabled
    with caplog.at_level(logging.INFO, logger="pglift.instances"):
        assert instances.apply(ctx, manifest._copy_validate({"data_checksums": False}))
    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "off"}]
    assert "disabling data checksums" in caplog.messages
    caplog.clear()
    assert instances._get(ctx, instance).data_checksums is False

    # re-enabled with instance running
    with instances.running(ctx, instance):
        with pytest.raises(
            exceptions.InstanceStateError,
            match="could not alter data_checksums on a running instance",
        ):
            instances.apply(ctx, manifest._copy_validate({"data_checksums": True}))
    assert instances._get(ctx, instance).data_checksums is False


@pytest.fixture
def extra_extensions(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    powa_available: bool,
) -> Iterator[Tuple[str, List[interface.Extension]]]:
    config = instance.config()
    if powa_available:
        spl_before = "passwordcheck, pg_qualstats, pg_stat_statements, pg_stat_kcache"
        spl = "pg_stat_statements, passwordcheck, pg_qualstats, pg_stat_kcache"
        extensions = ["pg_stat_statements", "unaccent", "passwordcheck"]
        expected_extensions = list(
            map(
                interface.Extension,
                [
                    "pg_stat_statements",
                    "passwordcheck",
                    "pg_qualstats",
                    "pg_stat_kcache",
                    "unaccent",
                ],
            )
        )
    else:
        spl_before = spl = "passwordcheck"
        extensions = ["unaccent", "passwordcheck"]
        expected_extensions = list(
            map(interface.Extension, ["passwordcheck", "unaccent"])
        )
    assert config.shared_preload_libraries == spl_before
    with instances.running(ctx, instance):
        new_manifest = instance_manifest._copy_validate({"extensions": extensions})
        r = instances.apply(ctx, new_manifest)
        instances.restart(ctx, instance)
        assert r is not None
        yield spl, expected_extensions
        instances.apply(ctx, instance_manifest)


def test_extensions(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    powa_available: bool,
    extra_extensions: Tuple[str, List[interface.Extension]],
) -> None:
    spl, expected_extensions = extra_extensions

    config = instance.config()
    assert config.shared_preload_libraries == spl

    def get_installed_extensions() -> List[str]:
        return [
            r["extname"]
            for r in execute(ctx, instance, "SELECT extname FROM pg_extension")
        ]

    installed = get_installed_extensions()
    if powa_available:
        assert "pg_stat_statements" in installed
    assert "unaccent" in installed

    # order of extensions as in shared_preload_libraries should be respected
    assert instances._get(ctx, instance).extensions == expected_extensions

    if powa_available:
        rows = execute(ctx, instance, "SELECT * FROM pg_stat_statements LIMIT 1")
        assert rows

    instances.apply(ctx, instance_manifest._copy_validate({"extensions": ["unaccent"]}))
    instances.restart(ctx, instance)
    config = instance.config()
    installed = get_installed_extensions()
    assert "unaccent" in installed
    if powa_available:
        assert "pg_stat_statements" not in installed
