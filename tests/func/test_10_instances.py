import logging
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

from . import execute, reconfigure_instance
from .conftest import DatabaseFactory


def test_init(
    ctx: Context,
    instance_initialized: system.PostgreSQLInstance,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    i = instance_initialized
    assert i.datadir.exists()
    assert i.waldir.exists()
    postgresql_conf = i.datadir / "postgresql.conf"
    assert postgresql_conf.exists()
    assert (i.waldir / "archive_status").is_dir()
    locale_prefix = "lc_"
    locale_settings = {}
    with postgresql_conf.open() as f:
        for line in f:
            if line.startswith(locale_prefix):
                key, value = line[len(locale_prefix) :].split(" = ", 1)
                locale_settings[key] = value.split("#", 1)[0].strip()
            else:
                sline = line.lstrip()
                assert not sline or sline.startswith(
                    "#"
                ), f"found uncommented line in postgresql.conf: {line}"

    expected_locale_settings = dict.fromkeys(
        ["messages", "monetary", "numeric", "time"], "'C'"
    )
    assert locale_settings == expected_locale_settings

    assert i.psqlrc.read_text().strip().splitlines() == [
        f"\\set PROMPT1 '[{i}] %n@%~%R%x%# '",
        "\\set PROMPT2 ' %R%x%# '",
    ]

    if ctx.settings.service_manager == "systemd":
        assert systemd.is_enabled(ctx, instances.systemd_unit(i))

    # Instance already exists, no-op.
    with monkeypatch.context() as m:

        def fail() -> NoReturn:
            raise AssertionError("unexpected called")

        m.setattr(instances, "pg_ctl", fail)
        instances.init(ctx, interface.Instance(name=i.name, version=i.version))


def test_log_directory(
    ctx: Context, instance: system.Instance, log_directory: Path
) -> None:
    config = instance.config()
    assert isinstance(config.log_directory, str)
    instance_log_dir = Path(config.log_directory)
    assert instance_log_dir == log_directory
    assert instance_log_dir.exists()


def test_pgpass(
    ctx: Context, instance_manifest: interface.Instance, instance: system.Instance
) -> None:
    port = instance.port
    passfile = ctx.settings.postgresql.auth.passfile

    def postgres_entry() -> str:
        (entry,) = [
            line for line in passfile.read_text().splitlines() if ":postgres:" in line
        ]
        return entry

    if instance_manifest.surole_password and ctx.settings.postgresql.surole.pgpass:
        assert postgres_entry() == f"*:{port}:*:postgres:s3kret"

        with reconfigure_instance(ctx, instance_manifest, port=port + 1):
            assert postgres_entry() == f"*:{port+1}:*:postgres:s3kret"

        assert postgres_entry() == f"*:{port}:*:postgres:s3kret"


def test_connect(
    ctx: Context, instance_manifest: interface.Instance, instance: system.Instance
) -> None:
    i = instance
    surole = instance_manifest.surole(ctx.settings)
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

    with instances.running(ctx, i):
        if password is not None:
            with pytest.raises(psycopg.OperationalError, match="no password supplied"):
                with patch.dict("os.environ", clear=True):
                    psycopg.connect(**connargs).close()  # type: ignore[call-overload]
            if password:
                connargs["password"] = password
            else:
                connargs["passfile"] = str(passfile)
        else:
            connargs["passfile"] = str(passfile)
        psycopg.connect(**connargs).close()  # type: ignore[call-overload]


def test_hba(ctx: Context, instance: system.Instance) -> None:
    hba_path = instance.datadir / "pg_hba.conf"
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


def test_ident(ctx: Context, instance: system.Instance) -> None:
    ident_path = instance.datadir / "pg_ident.conf"
    ident = ident_path.read_text().splitlines()
    assert ident == ["# MAPNAME       SYSTEM-USERNAME         PG-USERNAME"]


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
    surole_password: Optional[str],
    composite_instance_model: Type[interface.Instance],
    caplog: pytest.LogCaptureFixture,
) -> None:
    port = next(tmp_port_factory)
    prometheus_port = next(tmp_port_factory)
    im = composite_instance_model(
        name="test_apply",
        version=pg_version,
        port=port,
        ssl=True,
        state=interface.InstanceState.stopped,
        configuration={"unix_socket_directories": str(tmp_path)},
        prometheus={"port": prometheus_port},
        surole_password=surole_password,
    )
    r = instances.apply(ctx, im)
    assert r is not None
    i, changes, needs_restart = r
    assert i is not None
    assert i.exists()
    assert i.port == port
    assert changes["port"] == (None, port)
    pgconfig = i.config()
    assert pgconfig
    assert pgconfig.ssl

    assert instances.status(ctx, i) == Status.not_running
    im.state = interface.InstanceState.started
    r = instances.apply(ctx, im)
    assert r is not None
    i, changes, needs_restart = r
    assert not changes
    assert not needs_restart
    assert instances.status(ctx, i) == Status.running

    im.configuration["listen_addresses"] = "*"  # requires restart
    im.configuration["autovacuum"] = False  # requires reload
    with caplog.at_level(logging.DEBUG, logger="pgflit"):
        r = instances.apply(ctx, im)
    assert (
        f"instance {i} needs restart due to parameter changes: listen_addresses"
        in caplog.messages
    )
    assert r is not None
    i, changes, needs_restart = r
    assert changes == {
        "listen_addresses": (None, "*"),
        "autovacuum": (None, False),
    }
    assert needs_restart
    assert instances.status(ctx, i) == Status.running

    im.state = interface.InstanceState.stopped
    instances.apply(ctx, im)
    assert instances.status(ctx, i) == Status.not_running

    im.state = interface.InstanceState.absent
    r = instances.apply(ctx, im)
    assert r is None
    with pytest.raises(exceptions.InstanceNotFound):
        i.exists()
    assert instances.status(ctx, i) == Status.unspecified_datadir


def test_get(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance: system.Instance,
    log_directory: Path,
) -> None:
    im = instances.get(ctx, instance.name, instance.version)
    assert im is not None
    assert im.name == "test"
    config = im.configuration
    assert im.port == instance.port
    if "log_directory" in config:
        logdir = config.pop("log_directory")
        assert logdir == str(log_directory)
    assert config == {
        "logging_collector": False,
        "shared_preload_libraries": "passwordcheck",
    }
    if int(instance.version) <= 10:
        assert im.data_checksums is None
    else:
        assert im.data_checksums is False
    assert im.state.name == "stopped"
    assert not im.surole_password
    assert im.extensions == ["passwordcheck"]

    if instance_manifest.surole_password:
        with instances.running(ctx, instance):
            im = instances.get(ctx, instance.name, instance.version)
            assert isinstance(im.surole_password, SecretStr)


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


@pytest.mark.parametrize("slot", ["standby", None], ids=["with-slot", "no-slot"])
def test_standby(
    ctx: Context,
    instance: system.Instance,
    instance_manifest: interface.Instance,
    settings: Settings,
    tmp_port_factory: Iterator[int],
    tmp_path_factory: pytest.TempPathFactory,
    database_factory: DatabaseFactory,
    composite_instance_model: Type[interface.Instance],
    pg_version: str,
    slot: str,
) -> None:
    socket_directory = settings.postgresql.socket_directory
    replrole = instance_manifest.replrole(settings)
    standby_for = f"host={socket_directory} port={instance.port} user={replrole.name}"
    if replrole.password:
        standby_for += f" password={replrole.password.get_secret_value()}"
    standby_manifest = composite_instance_model(
        name="standby",
        version=pg_version,
        port=next(tmp_port_factory),
        standby=interface.Instance.Standby(**{"for": standby_for, "slot": slot}),
    )

    def pg_replication_slots() -> List[str]:
        rows = execute(ctx, instance, "SELECT slot_name FROM pg_replication_slots")
        return [r["slot_name"] for r in rows]

    with instances.running(ctx, instance):
        database_factory("test")
        execute(
            ctx,
            instance,
            "CREATE TABLE t AS (SELECT 1 AS i)",
            dbname="test",
            fetch=False,
            role=replrole,
        )
        assert not pg_replication_slots()
        r = instances.apply(ctx, standby_manifest)
        assert r is not None
        standby_instance = r[0]
        if slot:
            assert pg_replication_slots() == [slot]
        else:
            assert not pg_replication_slots()
        assert standby_instance.standby
        assert standby_instance.standby.for_
        assert standby_instance.standby.slot == slot

        surole = instance_manifest.surole(settings)
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

        stdby = get_stdby()
        assert stdby is not None
        assert stdby.for_ == standby_instance.standby.for_
        assert stdby.slot == standby_instance.standby.slot
        assert stdby.replication_lag is not None

        class OutOfSync(AssertionError):
            pass

        try:
            with instances.running(ctx, standby_instance):
                assert execute(
                    ctx,
                    standby_instance,
                    "SELECT * FROM pg_is_in_recovery()",
                    role=replrole,
                    dbname="template1",
                ) == [{"pg_is_in_recovery": True}]
                assert execute(
                    ctx,
                    standby_instance,
                    "SELECT * FROM t",
                    role=replrole,
                    dbname="test",
                ) == [{"i": 1}]
                execute(
                    ctx,
                    instance,
                    "UPDATE t SET i = 42",
                    dbname="test",
                    role=replrole,
                    fetch=False,
                )

                @retry(
                    retry=retry_if_exception_type(OutOfSync),
                    wait=wait_fixed(1),
                    stop=stop_after_attempt(4),
                )
                def assert_replicated() -> None:
                    rlag = instances.replication_lag(ctx, standby_instance)
                    assert rlag is not None
                    row = execute(
                        ctx,
                        standby_instance,
                        "SELECT * FROM t",
                        role=replrole,
                        dbname="test",
                    )
                    if row[0] == {"i": 1}:
                        assert rlag > 0
                        raise OutOfSync
                    assert row == [{"i": 42}]
                    if rlag > 0:
                        raise OutOfSync
                    assert rlag == 0

                assert_replicated()

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
        finally:
            instances.drop(ctx, standby_instance)
            if slot:
                execute(
                    ctx,
                    instance,
                    f"SELECT true FROM pg_drop_replication_slot('{slot}')",
                )
            assert not pg_replication_slots()


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


def test_get_locale(
    ctx: Context, instance_manifest: interface.Instance, instance: system.Instance
) -> None:
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


@pytest.fixture
def datachecksums_instance(
    ctx: Context,
    composite_instance_model: Type[interface.Instance],
    pg_version: str,
    tmp_port_factory: Iterator[int],
    surole_password: Optional[str],
) -> Iterator[Tuple[interface.Instance, system.Instance]]:
    manifest = composite_instance_model(
        name="datachecksums",
        version=pg_version,
        port=next(tmp_port_factory),
        state=interface.InstanceState.stopped,
        surole_password=surole_password,
    )
    r = instances.apply(ctx, manifest)
    assert r
    instance = r[0]
    yield manifest, instance
    instances.drop(ctx, instance)


def test_data_checksums(
    ctx: Context,
    pg_version: str,
    datachecksums_instance: Tuple[interface.Instance, system.Instance],
) -> None:
    manifest, instance = datachecksums_instance

    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "off"}]

    # explicitly enabled
    manifest = manifest.copy(
        update={
            "data_checksums": True,
            "state": interface.InstanceState.stopped,
        }
    )
    if int(pg_version) < 12:
        with pytest.raises(
            exceptions.UnsupportedError,
            match={
                "10": r"^PostgreSQL <= 10 doesn't allow to offline check for data-checksums$",
                "11": r"^PostgreSQL <= 11 doesn't have pg_checksums to enable data checksums$",
            }[pg_version],
        ):
            result = instances.apply(ctx, manifest)
        return

    result = instances.apply(ctx, manifest)
    assert result
    _, changes, _ = result
    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "on"}]
    assert changes == {
        "data_checksums": ("disabled", "enabled"),
    }

    assert instances._get(ctx, instance).data_checksums

    # not explicitly disabled so still enabled
    manifest = manifest.copy(update={"data_checksums": None})
    result = instances.apply(ctx, manifest)
    assert result
    _, changes, _ = result
    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "on"}]
    assert changes == {}

    # explicitly disabled
    manifest = manifest.copy(update={"data_checksums": False})
    result = instances.apply(ctx, manifest)
    assert result
    _, changes, _ = result
    assert execute(ctx, instance, "SHOW data_checksums") == [{"data_checksums": "off"}]
    assert changes == {
        "data_checksums": ("enabled", "disabled"),
    }
    assert instances._get(ctx, instance).data_checksums is False

    # re-enabled with instance running
    with instances.running(ctx, instance):
        manifest = manifest.copy(update={"data_checksums": True})
        with pytest.raises(
            exceptions.InstanceStateError,
            match="could not alter data_checksums on a running instance",
        ):
            instances.apply(ctx, manifest)
    assert instances._get(ctx, instance).data_checksums is False


def test_extensions(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance: system.Instance,
) -> None:
    config = instance.config()
    assert config.shared_preload_libraries == "passwordcheck"
    with instances.running(ctx, instance):
        manifest = instance_manifest.copy(
            update={"extensions": ["passwordcheck", "pg_stat_statements", "unaccent"]}
        )
        r = instances.apply(ctx, manifest)
        instances.restart(ctx, instance)
        assert r is not None

        im = instances.get(ctx, instance.name, instance.version)
        assert sorted(im.extensions) == [
            "passwordcheck",
            "pg_stat_statements",
            "unaccent",
        ]

        config = instance.config()
        assert config.shared_preload_libraries == "passwordcheck, pg_stat_statements"

        def get_installed_extensions() -> List[str]:
            return [
                r["extname"]
                for r in execute(ctx, instance, "SELECT extname FROM pg_extension")
            ]

        installed = get_installed_extensions()
        assert "pg_stat_statements" in installed
        assert "unaccent" in installed

        rows = execute(ctx, instance, "SELECT * FROM pg_stat_statements LIMIT 1")
        assert len(rows)

        manifest = manifest.copy(update={"extensions": ["unaccent"]})
        r = instances.apply(ctx, manifest)
        instances.restart(ctx, instance)
        config = instance.config()
        assert "shared_preload_libraries" not in config
        installed = get_installed_extensions()
        assert "pg_stat_statements" not in installed
        assert "unaccent" in installed
