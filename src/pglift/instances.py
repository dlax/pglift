import builtins
import contextlib
import functools
import logging
import os
import shutil
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import psycopg.rows
import psycopg.sql
from pgtoolkit import conf as pgconf
from pgtoolkit import ctl, pgpass
from pgtoolkit.ctl import Status as Status
from pydantic import SecretStr

from . import cmd, conf, databases, db, exceptions, hookimpl, roles, systemd, util
from ._compat import Literal
from .models import interface, system
from .settings import EXTENSIONS_CONFIG, PostgreSQLVersion, Settings
from .task import task
from .types import ConfigChanges

if TYPE_CHECKING:
    from .ctx import BaseContext

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=len(PostgreSQLVersion) + 1)
def pg_ctl(version: Optional[PostgreSQLVersion], *, ctx: "BaseContext") -> ctl.PGCtl:
    pg_bindir = None
    settings = ctx.settings.postgresql
    version = version or settings.default_version
    if version is not None and settings.versions:
        pg_bindir = settings.versions[version].bindir
    try:
        pg_ctl = ctl.PGCtl(pg_bindir, run_command=ctx.run)
    except EnvironmentError as e:
        raise exceptions.SystemError(
            f"{str(e)}. Is PostgreSQL {version} installed?"
        ) from e
    if version is not None:
        installed_version = util.short_version(pg_ctl.version)
        if installed_version != version:
            raise exceptions.SystemError(
                f"PostgreSQL version from {pg_bindir} mismatches with declared value: "
                f"{installed_version} != {version}"
            )
    return pg_ctl


def systemd_unit(instance: system.BaseInstance) -> str:
    return f"pglift-postgresql@{instance.version}-{instance.name}.service"


@hookimpl(trylast=True)  # type: ignore[misc]
def instance_init_replication(
    ctx: "BaseContext",
    instance: system.BaseInstance,
    standby: interface.Instance.Standby,
) -> Optional[bool]:
    with tempfile.TemporaryDirectory() as _tmpdir:
        tmpdir = Path(_tmpdir)
        # pg_basebackup will also copy config files from primary datadir.
        # So to have expected configuration at this stage we have to backup
        # postgresql.conf & pg_hba.conf (created by prior pg_ctl init) and
        # restore after pg_basebackup finishes.
        keep = {"postgresql.conf", "pg_hba.conf"}
        for name in keep:
            shutil.copyfile(instance.datadir / name, tmpdir / name)
        ctx.rmtree(instance.datadir)
        ctx.rmtree(instance.waldir)
        bindir = pg_ctl(instance.version, ctx=ctx).bindir
        cmd = [
            str(bindir / "pg_basebackup"),
            "--pgdata",
            str(instance.datadir),
            "--write-recovery-conf",
            "--checkpoint=fast",
            "--no-password",
            "--progress",
            "--verbose",
            "--dbname",
            standby.primary_conninfo,
            "--waldir",
            str(instance.waldir),
        ]

        if standby.slot:
            cmd += ["--slot", standby.slot]
            with db.connect_dsn(standby.primary_conninfo, dbname="template1") as cnx:
                # ensure the replication slot does not exists
                # otherwise --create-slot will raise an error
                cnx.execute(db.query("drop_replication_slot"), {"slot": standby.slot})
                if int(instance.version) <= 10:
                    cnx.execute(
                        db.query("create_replication_slot"), {"slot": standby.slot}
                    )
                else:
                    cmd += ["--create-slot"]

        ctx.run(cmd, check=True)
        for name in keep:
            shutil.copyfile(tmpdir / name, instance.datadir / name)
    return True


@task("initializing PostgreSQL instance")
def init(ctx: "BaseContext", manifest: interface.Instance) -> None:
    """Initialize a PostgreSQL instance."""
    settings = ctx.settings.postgresql
    initdb_settings = settings.initdb
    surole = settings.surole

    if exists(ctx, manifest.name, manifest.version):
        return None

    sys_instance = system.BaseInstance.get(manifest.name, manifest.version, ctx)

    # Would raise SystemError if requested postgresql binaries are not
    # available or if versions mismatch.
    pgctl = pg_ctl(manifest.version, ctx=ctx)

    pgroot = settings.root
    pgroot.parent.mkdir(parents=True, exist_ok=True)
    pgroot.mkdir(mode=0o750, exist_ok=True)

    settings.socket_directory.mkdir(parents=True, exist_ok=True)

    opts: Dict[str, Union[str, Literal[True]]] = {
        "waldir": str(sys_instance.waldir),
        "username": surole.name,
        # Set temporary auth methods, until the complete pg_hba.conf gets
        # deployed.
        "auth_local": "trust",
        "auth_host": "reject",
    }
    opts.update(manifest.initdb_options(initdb_settings).dict(exclude_none=True))

    surole_password = manifest.surole(ctx.settings).password
    if surole_password:
        with tempfile.NamedTemporaryFile("w") as pwfile:
            pwfile.write(surole_password.get_secret_value())
            pwfile.flush()
            pgctl.init(sys_instance.datadir, pwfile=pwfile.name, **opts)
    else:
        pgctl.init(sys_instance.datadir, **opts)

    # Possibly comment out everything in postgresql.conf, as in upstream
    # sample file, but in contrast with some distribution packages.
    postgresql_conf = sys_instance.datadir / "postgresql.conf"
    pgconfig = pgconf.Configuration(str(postgresql_conf))
    with postgresql_conf.open() as f:
        includes = builtins.list(pgconfig.parse(f))
    assert (
        not includes
    ), "default postgresql.conf contains unexpected include directives"
    with pgconfig.edit() as entries:
        commented = set()
        for name, entry in entries.items():
            if not entry.commented:
                entry.commented = True
                commented.add(name)
    logger.debug(
        "commenting PostgreSQL configuration entries in %s: %s",
        postgresql_conf,
        ", ".join(sorted(commented)),
    )
    pgconfig.save()

    standby = manifest.standby
    if standby:
        ctx.hook.instance_init_replication(
            ctx=ctx, instance=sys_instance, standby=standby
        )

    sys_instance.psqlrc.write_text(
        "\n".join(
            [
                f"\\set PROMPT1 '[{sys_instance}] %n@%~%R%x%# '",
                "\\set PROMPT2 ' %R%x%# '",
            ]
        )
        + "\n"
    )

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(sys_instance))

    return None


@init.revert("deleting PostgreSQL instance")
def revert_init(ctx: "BaseContext", manifest: interface.Instance) -> None:
    """Un-initialize a PostgreSQL instance."""
    sys_instance = system.BaseInstance.get(manifest.name, manifest.version, ctx)
    if ctx.settings.service_manager == "systemd":
        systemd.disable(ctx, systemd_unit(sys_instance), now=True)

    settings = ctx.settings.postgresql
    if sys_instance.path.exists():
        ctx.rmtree(sys_instance.path)
    pgroot = settings.root
    if pgroot.exists():
        try:
            next(pgroot.iterdir())
        except StopIteration:
            # directory is empty
            pgroot.rmdir()


@task("configuring PostgreSQL instance")
def configure(
    ctx: "BaseContext",
    manifest: interface.Instance,
    *,
    run_hooks: bool = True,
    _creating: bool = False,
) -> ConfigChanges:
    """Write instance's configuration in postgresql.conf.

    `manifest.ssl` parameter controls SSL configuration. If False, SSL is not
    enabled. If True, a self-signed certificate is generated. A tuple of two
    `~pathlib.Path` corresponding to the location of SSL cert file and key
    file to use may also be passed.

    Also compute changes to the overall PostgreSQL configuration and return it
    as a 'ConfigChanges' dictionary.
    """
    sys_instance = system.PostgreSQLInstance.system_lookup(
        ctx, (manifest.name, manifest.version)
    )
    datadir = sys_instance.datadir
    configure_ssl(ctx, manifest.configuration, datadir)
    config = configuration(ctx, manifest)

    if "log_directory" in config:
        logdir = Path(config.log_directory)  # type: ignore[arg-type]
        logdir.mkdir(exist_ok=True, parents=True)

    postgresql_conf = pgconf.parse(datadir / "postgresql.conf")
    changes: ConfigChanges = {}
    config_before = postgresql_conf.as_dict()
    conf.update(postgresql_conf, **config.as_dict())
    config_after = postgresql_conf.as_dict()
    for k in set(config_before) | set(config_after):
        pv = config_before.get(k)
        nv = config_after.get(k)
        if nv != pv:
            changes[k] = (pv, nv)

    if changes:
        postgresql_conf.save()

    if run_hooks:
        ctx.hook.instance_configure(
            ctx=ctx,
            manifest=manifest,
            config=config,
            changes=changes,
            creating=_creating,
        )

    if not _creating:
        sys_instance = system.Instance.system_lookup(
            ctx, (manifest.name, manifest.version)
        )
        check_pending_actions(ctx, sys_instance, changes, manifest.restart_on_changes)

    return changes


def configure_ssl(
    ctx: "BaseContext", configuration: Dict[str, Any], datadir: Path
) -> None:
    """Possibly generate SSL certificate files in instance 'datadir' based on specified 'configuration'."""
    if not configuration.get("ssl"):
        return
    try:
        cert, key = Path(configuration["ssl_cert_file"]), Path(
            configuration["ssl_key_file"]
        )
    except KeyError:
        cert, key = Path("server.crt"), Path("server.key")
    if not cert.is_absolute():
        cert = datadir / cert
    if not key.is_absolute():
        key = datadir / key
    if not cert.exists() and not key.exists():
        certcontent, keycontent = util.generate_certificate(
            run_command=functools.partial(ctx.run, log_output=False)
        )
        cert.write_text(certcontent)
        key.touch(mode=0o600)
        key.write_text(keycontent)
    else:
        assert (
            cert.exists() and key.exists()
        ), f"One of SSL certificate files {cert} or {key} exists but the other does not"


def configuration(
    ctx: "BaseContext", manifest: interface.Instance
) -> pgconf.Configuration:
    """Return instance configuration from manifest.

    'shared_buffers' and 'effective_cache_size' setting, if defined and set to
    a percent-value, will be converted to proper memory value relative to the
    total memory available on the system.
    """
    confitems: Dict[str, pgconf.Value] = {
        "cluster_name": manifest.name,
        "port": manifest.port,
    }

    # Load base configuration from site settings.
    postgresql_conf_template = util.site_config("postgresql", "postgresql.conf")
    if postgresql_conf_template is not None:
        confitems.update(pgconf.parse(postgresql_conf_template).as_dict())

    # Transform initdb options as configuration parameters.
    locale = manifest.initdb_options(ctx.settings.postgresql.initdb).locale
    if locale:
        for key in ("lc_messages", "lc_monetary", "lc_numeric", "lc_time"):
            confitems.setdefault(key, locale)

    confitems.update(manifest.configuration)

    spl = ""
    spl_list = []
    for extension in manifest.extensions:
        if EXTENSIONS_CONFIG[extension][0]:
            spl_list.append(extension)
    spl = ", ".join(spl_list)

    for r in ctx.hook.instance_configuration(ctx=ctx, manifest=manifest):
        for k, v in r.entries.items():
            if k == "shared_preload_libraries":
                spl = conf.merge_lists(spl, v.value)
            else:
                confitems[k] = v.value

    if spl:
        confitems["shared_preload_libraries"] = spl

    conf.format_values(confitems, ctx.settings.postgresql)

    return conf.make(manifest.name, **confitems)


@contextlib.contextmanager
def running(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    *,
    timeout: int = 10,
    run_hooks: bool = False,
) -> Iterator[None]:
    """Context manager to temporarily start an instance.

    :param timeout: delay to wait for instance startup.
    :param run_hooks: whether or not to run hooks during instance start/stop.

    :raises ~exceptions.InstanceStateError: when the instance did not
        get through running state after specified `timeout` (in seconds).
    """
    if status(ctx, instance) == Status.running:
        yield
        return

    if run_hooks and not isinstance(instance, system.Instance):
        raise TypeError("expecting a full instance")

    start(ctx, instance, run_hooks=run_hooks)
    for __ in range(timeout):
        time.sleep(1)
        if status(ctx, instance) == Status.running:
            break
    else:
        raise exceptions.InstanceStateError(f"{instance} not started after {timeout}s")
    try:
        yield
    finally:
        stop(ctx, instance, run_hooks=run_hooks)


@contextlib.contextmanager
def stopped(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    *,
    timeout: int = 10,
    run_hooks: bool = False,
) -> Iterator[None]:
    """Context manager to temporarily stop an instance.

    :param timeout: delay to wait for instance stop.
    :param run_hooks: whether or not to run hooks during instance start/stop.

    :raises ~exceptions.InstanceStateError: when the instance did stop after
        specified `timeout` (in seconds).
    """
    if status(ctx, instance) == Status.not_running:
        yield
        return

    if run_hooks and not isinstance(instance, system.Instance):
        raise TypeError("expecting a full instance")

    stop(ctx, instance, run_hooks=run_hooks)
    for __ in range(timeout):
        time.sleep(1)
        if status(ctx, instance) == Status.not_running:
            break
    else:
        raise exceptions.InstanceStateError(f"{instance} not stopped after {timeout}s")
    try:
        yield
    finally:
        start(ctx, instance, run_hooks=run_hooks)


def configure_auth(
    settings: Settings,
    instance: system.PostgreSQLInstance,
    manifest: interface.Instance,
) -> None:
    """Configure authentication for the PostgreSQL instance."""
    logger.info("configuring PostgreSQL authentication")
    hba_path = instance.datadir / "pg_hba.conf"
    hba = manifest.pg_hba(settings)
    hba_path.write_text(hba)

    ident_path = instance.datadir / "pg_ident.conf"
    ident = manifest.pg_ident(settings)
    ident_path.write_text(ident)


def start(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    *,
    wait: bool = True,
    logfile: Optional[Path] = None,
    run_hooks: bool = True,
    foreground: bool = False,
) -> None:
    """Start an instance.

    :param run_hooks: controls whether start-up hook will be triggered or not.
    :param foreground: start postgres in the foreground, replacing the current
        process.

    .. note:: When starting in "foreground", hooks will not be triggered and
        `wait` and `logfile` parameters have no effect.
    """
    if run_hooks and not isinstance(instance, system.Instance):
        raise TypeError("expecting a full instance")

    logger.info("starting instance %s", instance)
    if foreground and run_hooks:
        logger.debug("not running hooks for a foreground start")
        run_hooks = False

    ctx.settings.postgresql.socket_directory.mkdir(parents=True, exist_ok=True)

    start_postgresql(ctx, instance, wait=wait, logfile=logfile, foreground=foreground)

    if run_hooks and wait:
        ctx.hook.instance_start(ctx=ctx, instance=instance)


@task("starting PostgreSQL instance")
def start_postgresql(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    *,
    wait: bool = True,
    logfile: Optional[Path] = None,
    foreground: bool = False,
) -> None:
    if ctx.settings.service_manager is None:
        pgctl = pg_ctl(instance.version, ctx=ctx)
        if foreground:
            postgres = pgctl.bindir / "postgres"
            cmd.execute_program(
                [str(postgres), "-D", str(instance.datadir)], logger=logger
            )
        else:
            pgctl.start(instance.datadir, wait=wait, logfile=logfile)
    elif ctx.settings.service_manager == "systemd":
        systemd.start(ctx, systemd_unit(instance))


def status(ctx: "BaseContext", instance: system.BaseInstance) -> Status:
    """Return the status of an instance."""
    logger.debug("get status of PostgreSQL instance %s", instance)
    return pg_ctl(instance.version, ctx=ctx).status(instance.datadir)


def is_running(ctx: "BaseContext", instance: system.BaseInstance) -> bool:
    """Return True if the instance is running based on its status."""
    return status(ctx, instance) == Status.running


def check_status(
    ctx: "BaseContext", instance: system.BaseInstance, expected: Status
) -> None:
    """Check actual instance status with respected to `expected` one.

    :raises ~exceptions.InstanceStateError: in case the actual status is not expected.
    """
    st = status(ctx, instance)
    if st != expected:
        raise exceptions.InstanceStateError(f"instance is {st.name}")


def stop(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    *,
    mode: str = "fast",
    wait: bool = True,
    run_hooks: bool = True,
) -> None:
    """Stop an instance.

    :param run_hooks: controls whether stop hook will be triggered or not.
    """
    if run_hooks and not isinstance(instance, system.Instance):
        raise TypeError("expecting a full instance")

    if status(ctx, instance) == Status.not_running:
        logger.warning("instance %s is already stopped", instance)
    else:
        stop_postgresql(ctx, instance, mode=mode, wait=wait)
    if run_hooks and wait:
        ctx.hook.instance_stop(ctx=ctx, instance=instance)


@task("stopping PostgreSQL instance")
def stop_postgresql(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    mode: str = "fast",
    wait: bool = True,
) -> None:
    logger.info("stopping instance %s", instance)
    if ctx.settings.service_manager is None:
        pg_ctl(instance.version, ctx=ctx).stop(instance.datadir, mode=mode, wait=wait)
    elif ctx.settings.service_manager == "systemd":
        systemd.stop(ctx, systemd_unit(instance))


@task("restarting PostgreSQL instance")
def restart(
    ctx: "BaseContext",
    instance: system.Instance,
    *,
    mode: str = "fast",
    wait: bool = True,
) -> None:
    """Restart an instance."""
    logger.info("restarting instance %s", instance)
    ctx.hook.instance_stop(ctx=ctx, instance=instance)
    if ctx.settings.service_manager is None:
        pg_ctl(instance.version, ctx=ctx).restart(
            instance.datadir, mode=mode, wait=wait
        )
    elif ctx.settings.service_manager == "systemd":
        systemd.restart(ctx, systemd_unit(instance))
    ctx.hook.instance_start(ctx=ctx, instance=instance)


@task("reloading PostgreSQL instance")
def reload(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
) -> None:
    """Reload an instance."""
    logger.info("reloading instance %s", instance)
    with db.connect(ctx, instance) as cnx:
        cnx.execute("SELECT pg_reload_conf()")


@task("promoting PostgreSQL instance")
def promote(ctx: "BaseContext", instance: system.PostgreSQLInstance) -> None:
    """Promote a standby instance"""
    if not instance.standby:
        raise exceptions.InstanceStateError(f"{instance} is not a standby")
    pgctl = pg_ctl(instance.version, ctx=ctx)
    ctx.run(
        [str(pgctl.pg_ctl), "promote", "-D", str(instance.datadir)],
        check=True,
    )


@task("upgrading PostgreSQL instance")
def upgrade(
    ctx: "BaseContext",
    instance: system.Instance,
    *,
    version: Optional[str] = None,
    name: Optional[str] = None,
    port: Optional[int] = None,
    jobs: Optional[int] = None,
    _instance_model: Optional[Type[interface.Instance]] = None,
) -> system.Instance:
    """Upgrade a primary instance using pg_upgrade"""
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    if version is None:
        version = system.default_postgresql_version(ctx)
    if (name is None or name == instance.name) and version == instance.version:
        raise exceptions.InvalidVersion(
            f"Could not upgrade {instance} using same name and same version"
        )
    # check if target name/version already exists
    if exists(ctx, name=(instance.name if name is None else name), version=version):
        raise exceptions.InstanceAlreadyExists(
            f"Could not upgrade {instance}: target name/version instance already exists"
        )

    if not ctx.confirm(
        f"Confirm upgrade of instance {instance} to version {version}?", True
    ):
        raise exceptions.Cancelled(f"upgrade of instance {instance} cancelled")

    postgresql_settings = ctx.settings.postgresql
    surole = postgresql_settings.surole
    surole_password = postgresql_settings.libpq_environ(ctx, instance).get("PGPASSWORD")
    if not surole_password and ctx.settings.postgresql.auth.passfile:
        with pgpass.edit(ctx.settings.postgresql.auth.passfile) as passfile:
            for entry in passfile:
                if entry.matches(port=instance.port, username=surole.name):
                    surole_password = entry.password
    if _instance_model is None:
        _instance_model = interface.Instance.composite(ctx.pm)
    new_manifest = _instance_model.parse_obj(
        dict(
            _get(ctx, instance),
            name=name or instance.name,
            version=version,
            port=port or instance.port,
            state=interface.InstanceState.stopped,
            surole_password=SecretStr(surole_password) if surole_password else None,
        )
    )
    init(ctx, new_manifest)
    configure(ctx, new_manifest, _creating=True, run_hooks=False)
    newinstance = system.Instance.system_lookup(
        ctx, (new_manifest.name, new_manifest.version)
    )
    bindir = pg_ctl(version, ctx=ctx).bindir
    pg_upgrade = str(bindir / "pg_upgrade")
    cmd = [
        pg_upgrade,
        f"--old-bindir={pg_ctl(instance.version, ctx=ctx).bindir}",
        f"--new-bindir={bindir}",
        f"--old-datadir={instance.datadir}",
        f"--new-datadir={newinstance.datadir}",
        f"--username={ctx.settings.postgresql.surole.name}",
    ]
    if jobs is not None:
        cmd.extend(["--jobs", str(jobs)])
    env = postgresql_settings.libpq_environ(ctx, instance)
    if surole_password:
        env.setdefault("PGPASSWORD", surole_password)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            ctx.run(cmd, check=True, cwd=tmpdir, env=env)
        apply(ctx, new_manifest, _creating=True)
    except exceptions.CommandError:
        drop(ctx, newinstance)
        raise
    return newinstance


def get_locale(
    ctx: "BaseContext", instance: system.PostgreSQLInstance
) -> Optional[str]:
    """Return the value of instance locale.

    If locale subcategories are set to distinct values, return None.

    The instance must be running.
    """
    locales = {
        s.name: s.setting for s in settings(ctx, instance) if s.name.startswith("lc_")
    }
    values = set(locales.values())
    if len(values) == 1:
        return values.pop()
    else:
        logger.debug(
            "cannot determine instance locale, settings are heterogeneous: %s",
            ", ".join(f"{n}: {s}" for n, s in sorted(locales.items())),
        )
        return None


def get_encoding(ctx: "BaseContext", instance: system.PostgreSQLInstance) -> str:
    """Return the value of instance encoding."""
    with db.connect(ctx, instance) as cnx:
        row = cnx.execute(db.query("instance_encoding")).fetchone()
        assert row is not None
        value = row["pg_encoding_to_char"]
        return str(value)


def get_data_checksums(ctx: "BaseContext", instance: system.PostgreSQLInstance) -> bool:
    """Return True/False if data_checksums is enabled/disabled on instance."""
    if status(ctx, instance) == Status.running:
        # Use SQL SHOW data_checksums since pg_checksums doesn't work if
        # instance is running.
        with db.connect(ctx, instance) as cnx:
            row = cnx.execute("SHOW data_checksums").fetchone()
            assert row is not None
            value = row["data_checksums"]
            assert value in ("on", "off"), value
            return True if value == "on" else False
    version = int(instance.version)
    if version <= 10:
        raise exceptions.UnsupportedError(
            "PostgreSQL <= 10 doesn't allow to offline check for data-checksums"
        )
    elif version == 11:
        command = str(pg_ctl(instance.version, ctx=ctx).bindir / "pg_verify_checksums")
        proc = ctx.run([command, "--pgdata", str(instance.datadir)])
    else:
        command = str(pg_ctl(instance.version, ctx=ctx).bindir / "pg_checksums")
        proc = ctx.run([command, "--check", "--pgdata", str(instance.datadir)])
    if proc.returncode == 0:
        return True
    elif proc.returncode == 1:
        return False
    raise exceptions.CommandError(proc.returncode, proc.args, proc.stdout, proc.stderr)


def set_data_checksums(
    ctx: "BaseContext", instance: system.PostgreSQLInstance, enabled: bool
) -> None:
    """Enable/disable data checksums on instance."""
    if status(ctx, instance) == Status.running:
        raise exceptions.InstanceStateError(
            "could not alter data_checksums on a running instance"
        )
    action = "enable" if enabled else "disable"
    if int(instance.version) < 12:
        raise exceptions.UnsupportedError(
            "PostgreSQL <= 11 doesn't have pg_checksums to enable data checksums"
        )
    ctx.run(
        [
            str(pg_ctl(instance.version, ctx=ctx).bindir / "pg_checksums"),
            f"--{action}",
            "--pgdata",
            str(instance.datadir),
        ],
        check=True,
    )


def apply(
    ctx: "BaseContext", instance: interface.Instance, *, _creating: bool = False
) -> Optional[bool]:
    """Apply state described by interface model as a PostgreSQL instance.

    Depending on the previous state and existence of the target instance, the
    instance may be created or updated or dropped.

    Return True, if changes were applied, False if no change is needed, and
    None if the instance got dropped.

    If configuration changes are detected and the instance was previously
    running, the server will be reloaded automatically; if a restart is
    needed, the user will be prompted in case of interactive usage or this
    will be performed automatically if 'restart_on_changes' is set to True.
    """
    States = interface.InstanceState
    state = instance.state

    if state == States.absent:
        if exists(ctx, instance.name, instance.version):
            drop(
                ctx,
                system.Instance.system_lookup(ctx, (instance.name, instance.version)),
            )
            return None
        return False

    changed = False
    if not exists(ctx, instance.name, instance.version):
        _creating = True
        interface.validate_ports(instance)
        init(ctx, instance)
        changed = True

    changes = configure(ctx, instance, _creating=_creating)
    changed = changed or bool(changes)

    sys_instance = system.Instance.system_lookup(ctx, (instance.name, instance.version))

    if _creating:
        replrole = instance.replrole(ctx.settings)
        if not sys_instance.standby:
            # standby instances are read-only
            logger.info("creating replication user %s", replrole.name)
            with running(ctx, sys_instance):
                roles.apply(ctx, sys_instance, replrole)
        configure_auth(ctx.settings, sys_instance, instance)

    instance_is_running = is_running(ctx, sys_instance)

    if instance.data_checksums is not None:
        actual_data_checksums = get_data_checksums(ctx, sys_instance)
        if actual_data_checksums != instance.data_checksums:
            if instance.data_checksums:
                logger.info("enabling data checksums")
            else:
                logger.info("disabling data checksums")
            set_data_checksums(ctx, sys_instance, instance.data_checksums)
            changed = True

    if state == States.stopped:
        if instance_is_running:
            stop(ctx, sys_instance)
            changed = True
    elif state in (States.started, States.restarted):
        if not instance_is_running:
            start(ctx, sys_instance)
            changed = True
        elif state == States.restarted:
            restart(ctx, sys_instance)
            changed = True
    else:
        assert False, f"unexpected state: {state}"  # pragma: nocover

    StandbyState = instance.Standby.State

    if (
        instance.standby
        and instance.standby.status == StandbyState.promoted
        and sys_instance.standby is not None
    ):
        promote(ctx, sys_instance)

    if not sys_instance.standby:
        with running(ctx, sys_instance):
            db.create_or_drop_extensions(ctx, sys_instance, instance.extensions)
            for a_role in instance.roles:
                changed = roles.apply(ctx, sys_instance, a_role) is not False or changed
            for a_database in instance.databases:
                changed = (
                    databases.apply(ctx, sys_instance, a_database) is not False
                    or changed
                )

    return changed


def pending_restart(ctx: "BaseContext", instance: system.PostgreSQLInstance) -> bool:
    """Return True if the instance is pending a restart to account for configuration changes."""
    if not is_running(ctx, instance):
        return False
    with db.connect(ctx, instance) as cnx, cnx.cursor(
        row_factory=psycopg.rows.args_row(bool)
    ) as cur:
        cur.execute("SELECT bool_or(pending_restart) FROM pg_settings")
        row = cur.fetchone()
        assert row is not None
        return row


def check_pending_actions(
    ctx: "BaseContext",
    instance: system.Instance,
    changes: ConfigChanges,
    restart_on_changes: bool,
) -> None:
    """Check if any of the changes require a reload or a restart.

    The instance is automatically reloaded if needed.
    The user is prompted for confirmation if a restart is needed.
    """
    if not is_running(ctx, instance):
        return

    if "port" in changes:
        needs_restart = True
    else:
        needs_restart = False
        pending_restart = set()
        pending_reload = set()
        for p in settings(ctx, instance):
            pname = p.name
            if pname not in changes:
                continue
            if p.context == "postmaster":
                pending_restart.add(pname)
            else:
                pending_reload.add(pname)

        if pending_reload:
            logger.info(
                "instance %s needs reload due to parameter changes: %s",
                instance,
                ", ".join(sorted(pending_reload)),
            )
            reload(ctx, instance)

        if pending_restart:
            logger.warning(
                "instance %s needs restart due to parameter changes: %s",
                instance,
                ", ".join(sorted(pending_restart)),
            )
            needs_restart = True

    if needs_restart and ctx.confirm(
        "Instance needs to be restarted; restart now?", restart_on_changes
    ):
        restart(ctx, instance)


def get(ctx: "BaseContext", name: str, version: Optional[str]) -> interface.Instance:
    """Return the instance object with specified name and version."""
    instance = system.Instance.system_lookup(ctx, (name, version))
    if not is_running(ctx, instance):
        missing_bits = [
            "locale",
            "encoding",
            "passwords",
            "extensions",
            "pending_restart",
        ]
        if instance.standby is not None:
            missing_bits.append("replication lag")
        logger.warning(
            "instance %s is not running, information about %s may not be accurate",
            instance,
            f"{', '.join(missing_bits[:-1])} and {missing_bits[-1]}",
        )
    return _get(ctx, instance)


def _get(ctx: "BaseContext", instance: system.Instance) -> interface.Instance:
    config = instance.config()
    managed_config = config.as_dict()
    managed_config.pop("port", None)
    st = status(ctx, instance)
    state = interface.InstanceState.from_pg_status(st)
    instance_is_running = is_running(ctx, instance)
    services = {
        s.__class__.__service__: s
        for s in ctx.hook.get(ctx=ctx, instance=instance)
        if s is not None
    }
    if instance.standby:
        kw: Dict[str, Any] = {
            "for": instance.standby.for_,
            "slot": instance.standby.slot,
            "password": instance.standby.password,
        }
        if instance_is_running:
            kw["replication_lag"] = replication_lag(ctx, instance)
        standby = interface.Instance.Standby(**kw)
    else:
        standby = None

    extensions: List[interface.Extension] = []
    if "shared_preload_libraries" in config:
        extensions += [
            interface.Extension(spl.strip())
            for spl in str(config["shared_preload_libraries"]).split(",")
            if spl.strip()
        ]

    locale = None
    encoding = None
    pending_rst = False
    if instance_is_running:
        locale = get_locale(ctx, instance)
        encoding = get_encoding(ctx, instance)
        extensions += [
            e for e in db.installed_extensions(ctx, instance) if e not in extensions
        ]
        pending_rst = pending_restart(ctx, instance)

    try:
        data_checksums = get_data_checksums(ctx, instance)
    except exceptions.UnsupportedError as e:
        logger.warning(str(e))
        data_checksums = None

    return interface.Instance(
        name=instance.name,
        version=instance.version,
        port=instance.port,
        state=state,
        pending_restart=pending_rst,
        ssl=config.get("ssl", False),
        configuration=managed_config,
        locale=locale,
        encoding=encoding,
        data_checksums=data_checksums,
        extensions=extensions,
        standby=standby,
        **services,
    )


@task("dropping PostgreSQL instance")
def drop(ctx: "BaseContext", instance: system.Instance) -> None:
    """Drop an instance."""
    if not ctx.confirm(f"Confirm complete deletion of instance {instance}?", True):
        raise exceptions.Cancelled(f"deletion of instance {instance} cancelled")

    stop(ctx, instance, run_hooks=True)

    ctx.hook.instance_drop(ctx=ctx, instance=instance)
    manifest = interface.Instance(name=instance.name, version=instance.version)
    revert_init(ctx, manifest)


def list(
    ctx: "BaseContext", *, version: Optional[PostgreSQLVersion] = None
) -> Iterator[interface.InstanceListItem]:
    """Yield instances found by system lookup.

    :param version: filter instances matching a given version.

    :raises ~exceptions.InvalidVersion: if specified version is unknown.
    """
    for instance in system_list(ctx, version=version):
        yield interface.InstanceListItem(
            name=instance.name,
            path=instance.path,
            port=instance.port,
            status=status(ctx, instance).name,
            version=instance.version,
        )


def system_list(
    ctx: "BaseContext", *, version: Optional[PostgreSQLVersion] = None
) -> Iterator[system.PostgreSQLInstance]:
    if version is not None:
        assert isinstance(version, PostgreSQLVersion)
        versions = [version.value]
    else:
        versions = builtins.list(ctx.settings.postgresql.versions)

    pgroot = ctx.settings.postgresql.root

    # Search for directories looking like <version>/<name> in pgroot
    for ver in versions:
        version_path = pgroot / ver
        if not version_path.is_dir():
            continue
        for d in version_path.iterdir():
            if not d.is_dir():
                continue
            try:
                yield system.PostgreSQLInstance.system_lookup(ctx, (d.name, ver))
            except exceptions.InstanceNotFound:
                pass


def env_for(
    ctx: "BaseContext", instance: system.PostgreSQLInstance, *, path: bool = False
) -> Dict[str, str]:
    """Return libpq environment variables suitable to connect to `instance`.

    If 'path' is True, also inject PostgreSQL binaries directory in PATH.
    """
    postgresql_settings = ctx.settings.postgresql
    env = postgresql_settings.libpq_environ(ctx, instance, base={})
    config = instance.config()
    try:
        host = config.unix_socket_directories.split(",")[0]  # type: ignore[union-attr]
    except (AttributeError, IndexError):
        host = "localhost"
    env.update(
        {
            "PGUSER": ctx.settings.postgresql.surole.name,
            "PGPORT": str(instance.port),
            "PGHOST": host,
            "PGDATA": str(instance.datadir),
            "PSQLRC": str(instance.psqlrc),
            "PSQL_HISTORY": str(instance.psql_history),
        }
    )
    if path:
        env["PATH"] = ":".join(
            [str(pg_ctl(instance.version, ctx=ctx).bindir)]
            + ([os.environ["PATH"]] if "PATH" in os.environ else [])
        )
    for env_vars in ctx.hook.instance_env(ctx=ctx, instance=instance):
        env.update(env_vars)
    return env


def exec(
    ctx: "BaseContext", instance: system.PostgreSQLInstance, command: Tuple[str, ...]
) -> None:
    """Execute given PostgreSQL command in the libpq environment for `instance`.

    The command to be executed is looked up for in PostgreSQL binaries directory.
    """
    env = os.environ.copy()
    env.update(env_for(ctx, instance))
    progname, *args = command
    program = pg_ctl(instance.version, ctx=ctx).bindir / progname
    try:
        cmd.execute_program([str(program)] + args, env=env, logger=logger)
    except FileNotFoundError as e:
        raise exceptions.FileNotFoundError(str(e))


def env(ctx: "BaseContext", instance: system.PostgreSQLInstance) -> str:
    return "\n".join(
        [
            f"export {key}={value}"
            for key, value in sorted(env_for(ctx, instance, path=True).items())
        ]
    )


def exists(ctx: "BaseContext", name: str, version: Optional[str]) -> bool:
    """Return true when instance exists"""
    try:
        system.PostgreSQLInstance.system_lookup(ctx, (name, version))
    except exceptions.InstanceNotFound:
        return False
    return True


def settings(
    ctx: "BaseContext", instance: system.PostgreSQLInstance
) -> List[interface.PGSetting]:
    """Return the list of run-time parameters of the server, as available in
    pg_settings view.

    The instance must be running.
    """
    with db.connect(ctx, instance, dbname="template1") as cnx, cnx.cursor(
        row_factory=psycopg.rows.class_row(interface.PGSetting)
    ) as cur:
        cur.execute(interface.PGSetting._query)
        return cur.fetchall()


def logs(ctx: "BaseContext", instance: system.PostgreSQLInstance) -> Iterator[str]:
    """Return the content of current log file as an iterator.

    :raises ~exceptions.FileNotFoundError: if the current log file, matching
        configured log_destination, is not found.
    :raises ~exceptions.SystemError: if the current log file cannot be opened
        for reading.
    :raises ValueError: if no record matching configured log_destination is
        found in current_logfiles (this indicates a misconfigured instance).
    """
    config = instance.config()
    log_destination = config.get("log_destination", "stderr")
    current_logfiles = instance.datadir / "current_logfiles"
    if not current_logfiles.exists():
        raise exceptions.FileNotFoundError(
            f"file 'current_logfiles' for instance {instance} not found"
        )
    with current_logfiles.open() as f:
        for line in f:
            destination, logfilelocation = line.strip().split(None, maxsplit=1)
            if destination == log_destination:
                break
        else:
            raise ValueError(
                f"no record matching '{log_destination}' log destination found for instance {instance}"
            )

    logfile = Path(logfilelocation)
    if not logfile.is_absolute():
        logfile = instance.datadir / logfile

    logger.info("reading logs of instance '%s' from %s", instance, logfile)
    try:
        with logfile.open() as f:
            yield from f
    except OSError:
        raise exceptions.SystemError(f"failed to read {logfile} on instance {instance}")


def replication_lag(
    ctx: "BaseContext", instance: system.PostgreSQLInstance
) -> Optional[Decimal]:
    """Return the replication lag of a standby instance.

    The instance must be running; if the primary is not running, None is
    returned.

    :raises TypeError: if the instance is not a standby.
    """
    standby = instance.standby
    if standby is None:
        raise TypeError(f"{instance} is not a standby")

    try:
        with db.primary_connect(standby) as cnx:
            row = cnx.execute("SELECT pg_current_wal_lsn() AS lsn").fetchone()
    except psycopg.OperationalError as e:
        logger.warning("failed to connect to primary (is it running?): %s", e)
        return None
    assert row is not None
    primary_lsn = row["lsn"]

    password = standby.password.get_secret_value() if standby.password else None
    dsn = db.dsn(
        instance,
        ctx.settings.postgresql,
        dbname="template1",
        user=ctx.settings.postgresql.replrole,
        password=password,
    )
    with db.connect_dsn(dsn, autocommit=True) as cnx:
        row = cnx.execute(
            "SELECT %s::pg_lsn - pg_last_wal_replay_lsn() AS lag", (primary_lsn,)
        ).fetchone()
    assert row is not None
    lag = row["lag"]
    assert isinstance(lag, Decimal)
    return lag
