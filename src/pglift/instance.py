import builtins
import contextlib
import functools
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import psycopg.rows
from pgtoolkit import conf as pgconf
from pgtoolkit import ctl, pgpass
from pgtoolkit.ctl import Status as Status
from pydantic import SecretStr
from typing_extensions import Literal

from . import cmd, conf, db, exceptions, hookimpl, roles, systemd, util
from .models import interface, system
from .settings import POSTGRESQL_SUPPORTED_VERSIONS
from .task import task
from .types import ConfigChanges

if TYPE_CHECKING:
    from .ctx import BaseContext

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=len(POSTGRESQL_SUPPORTED_VERSIONS) + 1)
def pg_ctl(version: Optional[str], *, ctx: "BaseContext") -> ctl.PGCtl:
    pg_bindir = None
    settings = ctx.settings.postgresql
    version = version or settings.default_version
    if version is not None:
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


def init_replication(
    ctx: "BaseContext",
    instance: system.BaseInstance,
    standby_for: str,
    slot: Optional[str],
) -> None:
    with tempfile.TemporaryDirectory() as _tmpdir:
        tmpdir = Path(_tmpdir)
        # pg_basebackup will also copy config files from primary datadir.
        # So to have expected configuration at this stage we have to backup
        # postgresql.conf & pg_hba.conf (created by prior pg_ctl init) and
        # restore after pg_basebackup finishes.
        keep = {"postgresql.conf", "pg_hba.conf"}
        for name in keep:
            shutil.copyfile(instance.datadir / name, tmpdir / name)
        shutil.rmtree(instance.datadir)
        shutil.rmtree(instance.waldir)
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
            standby_for,
            "--waldir",
            str(instance.waldir),
        ]

        if slot:
            cmd += ["--slot", slot]
            with db.connect_dsn(
                standby_for,
                dbname="template1",
            ) as cnx:
                # ensure the replication slot does not exists
                # otherwise --create-slot will raise an error
                cnx.execute(db.query("drop_replication_slot"), {"slot": slot})
                if int(instance.version) <= 10:
                    cnx.execute(db.query("create_replication_slot"), {"slot": slot})
                else:
                    cmd += ["--create-slot"]

        ctx.run(cmd, check=True)
        for name in keep:
            shutil.copyfile(tmpdir / name, instance.datadir / name)
        # When primary is also managed by pglift, pg_basebackup will also copy
        # conf.pglift.d. So we must drop it to not interfer with site/user
        # config generated by instance_configure
        shutil.rmtree(instance.datadir / "conf.pglift.d", ignore_errors=True)


@task("initializing PostgreSQL instance")
def init(ctx: "BaseContext", manifest: interface.Instance) -> None:
    """Initialize a PostgreSQL instance."""
    settings = ctx.settings.postgresql
    initdb_settings = settings.initdb
    surole = settings.surole

    if exists(ctx, manifest.name, manifest.version):
        return None

    instance = system.BaseInstance.get(manifest.name, manifest.version, ctx)

    # Would raise SystemError if requested postgresql binaries are not
    # available or if versions mismatch.
    pgctl = pg_ctl(manifest.version, ctx=ctx)

    pgroot = settings.root
    pgroot.parent.mkdir(parents=True, exist_ok=True)
    pgroot.mkdir(mode=0o750, exist_ok=True)

    settings.socket_directory.mkdir(parents=True, exist_ok=True)

    opts: Dict[str, Union[str, Literal[True]]] = {
        "waldir": str(instance.waldir),
        "username": surole.name,
        "encoding": "UTF8",
        # Set temporary auth methods, until the complete pg_hba.conf gets
        # deployed.
        "auth_local": "trust",
        "auth_host": "reject",
    }
    if initdb_settings.locale:
        opts["locale"] = initdb_settings.locale
    if manifest.data_checksums or (
        manifest.data_checksums is None and initdb_settings.data_checksums
    ):
        opts["data_checksums"] = True

    surole_password = manifest.surole(ctx.settings).password
    if surole_password:
        with tempfile.NamedTemporaryFile("w") as pwfile:
            pwfile.write(surole_password.get_secret_value())
            pwfile.flush()
            pgctl.init(instance.datadir, pwfile=pwfile.name, **opts)
    else:
        pgctl.init(instance.datadir, **opts)

    # Possibly comment out everything in postgresql.conf, as in upstream
    # sample file, but in contrast with some distribution packages.
    postgresql_conf = instance.datadir / "postgresql.conf"
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

    if manifest.standby:
        init_replication(ctx, instance, manifest.standby.for_, manifest.standby.slot)

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(instance))

    return None


@init.revert("deleting PostgreSQL instance")
def revert_init(ctx: "BaseContext", manifest: interface.Instance) -> None:
    """Un-initialize a PostgreSQL instance."""
    instance = system.BaseInstance.get(manifest.name, manifest.version, ctx)
    if ctx.settings.service_manager == "systemd":
        systemd.disable(ctx, systemd_unit(instance), now=True)

    settings = ctx.settings.postgresql
    if instance.path.exists():
        shutil.rmtree(instance.path)
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
    ssl: Union[bool, Tuple[Path, Path]] = False,
    values: Optional[Mapping[str, Optional[pgconf.Value]]] = None,
    _creating: bool = False,
) -> ConfigChanges:
    """Write instance's configuration and include it in its postgresql.conf.

    `ssl` parameter controls SSL configuration. If False, SSL is not enabled.
    If True, a self-signed certificate is generated. A tuple of two
    `~pathlib.Path` corresponding to the location of SSL cert file and key
    file to use may also be passed.

    `values` defines configuration items to be set in managed configuration
    file. This should be a *complete definition*: any item present in the
    configuration file and absent from this mapping will be dropped.

    'shared_buffers' and 'effective_cache_size' setting, if defined and set to
    a percent-value, will be converted to proper memory value relative to the
    total memory available on the system.
    """
    instance = system.PostgreSQLInstance.system_lookup(
        ctx, (manifest.name, manifest.version)
    )
    configdir = instance.datadir
    postgresql_conf = configdir / "postgresql.conf"
    confd, include = conf.info(configdir)
    if not confd.exists():
        confd.mkdir()
    site_conffile = confd / "site.conf"
    user_conffile = confd / "user.conf"
    pgconfig = pgconf.parse(str(postgresql_conf))
    confitems = dict(values or {})
    if ssl:
        confitems["ssl"] = True
    if not pgconfig.get("ssl", False) and ssl is True:
        util.generate_certificate(configdir, run_command=ctx.run)
    elif isinstance(ssl, tuple):
        try:
            certfile, keyfile = ssl
        except ValueError:
            raise ValueError("expecting a 2-tuple for 'ssl' parameter") from None
        confitems["ssl_cert_file"] = str(certfile)
        confitems["ssl_key_file"] = str(keyfile)
    original_content = postgresql_conf.read_text()
    if not any(line.startswith(include) for line in original_content.splitlines()):
        with postgresql_conf.open("w") as f:
            f.write(f"{include}\n\n")
            f.write(original_content)

    site_confitems: Dict[str, Optional[pgconf.Value]] = {"cluster_name": instance.name}
    site_config_template = util.site_config("postgresql", "site.conf")
    if site_config_template is None:
        site_config_template = util.datapath / "postgresql" / "site.conf"
    if site_config_template.exists():
        site_confitems.update(pgconf.parse(site_config_template).as_dict())

    def format_values(
        confitems: Dict[str, Any], memtotal: float = util.total_memory()
    ) -> None:
        for k in ("shared_buffers", "effective_cache_size"):
            try:
                v = confitems[k]
            except KeyError:
                continue
            if v is None:
                continue
            try:
                confitems[k] = util.percent_memory(v, memtotal)
            except ValueError:
                pass
        for k, v in confitems.items():
            if isinstance(v, str):
                confitems[k] = v.format(settings=ctx.settings.postgresql)

    format_values(confitems)
    format_values(site_confitems)

    def make_config(
        fpath: Path, items: Dict[str, Optional[pgconf.Value]]
    ) -> Tuple[pgconf.Configuration, ConfigChanges]:
        config = conf.make(instance.name, **items)

        config_before = {}
        if fpath.exists():
            config_before = {e.name: e.value for e in pgconf.parse(fpath)}
        config_after = {e.name: e.value for e in config}
        changes: ConfigChanges = {}
        for k in set(config_before) | set(config_after):
            pv = config_before.get(k)
            nv = config_after.get(k)
            if nv != pv:
                changes[k] = (pv, nv)

        return config, changes

    site_config, site_changes = make_config(site_conffile, site_confitems)
    user_config, changes = make_config(user_conffile, confitems)

    def write_configs() -> None:
        if site_changes:
            with site_conffile.open("w") as f:
                site_config.save(f)
        if changes:
            with user_conffile.open("w") as f:
                user_config.save(f)

    if _creating:
        write_configs()
    i_config = site_config + user_config
    ctx.hook.instance_configure(
        ctx=ctx, manifest=manifest, config=i_config, changes=changes
    )
    if not _creating:
        write_configs()

    if "log_directory" in i_config:
        logdir = Path(i_config.log_directory)  # type: ignore[arg-type]
        conf.create_log_directory(instance, logdir)

    return changes


@contextlib.contextmanager
def running(
    ctx: "BaseContext",
    instance: Union[system.PostgreSQLInstance, system.Instance],
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
    instance: Union[system.PostgreSQLInstance, system.Instance],
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


@hookimpl  # type: ignore[misc]
def instance_configure(ctx: "BaseContext", manifest: interface.Instance) -> None:
    """Configure authentication for the PostgreSQL instance by setting
    super-user role's password, if any, and installing templated pg_hba.conf
    and pg_ident.conf.

    This is a no-op if if pg_hba.conf's content matches the initial
    configuration.
    """
    logger.info("configuring PostgreSQL authentication")
    surole = manifest.surole(ctx.settings)
    replrole = manifest.replrole(ctx.settings)
    auth_settings = ctx.settings.postgresql.auth
    instance = system.Instance.system_lookup(ctx, (manifest.name, manifest.version))
    hba_path = instance.datadir / "pg_hba.conf"
    hba = util.template("postgresql", "pg_hba.conf").format(
        surole=surole.name,
        replrole=replrole.name,
        auth_local=auth_settings.local,
        auth_host=auth_settings.host,
    )
    if hba_path.read_text() == hba:
        return

    if not instance.standby:
        # standby instances are read-only
        with running(ctx, instance):
            roles.apply(ctx, instance, replrole)

    hba_path.write_text(hba)

    ident_path = instance.datadir / "pg_ident.conf"
    ident = util.template("postgresql", "pg_ident.conf")
    ident_path.write_text(ident)


def start(
    ctx: "BaseContext",
    instance: Union[system.PostgreSQLInstance, system.Instance],
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

    start_postgresql(ctx, instance, wait=wait, logfile=logfile, foreground=foreground)

    if run_hooks and wait:
        ctx.hook.instance_start(ctx=ctx, instance=instance)


@task("starting PostgreSQL instance")
def start_postgresql(
    ctx: "BaseContext",
    instance: Union[system.PostgreSQLInstance, system.Instance],
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
    instance: Union[system.PostgreSQLInstance, system.Instance],
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
    instance: Union[system.PostgreSQLInstance, system.Instance],
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
    instance: system.Instance,
) -> None:
    """Reload an instance."""
    logger.info("reloading instance %s", instance)
    with db.superuser_connect(ctx, instance) as cnx:
        cnx.execute("SELECT pg_reload_conf()")


@task("promoting PostgreSQL instance")
def promote(ctx: "BaseContext", instance: system.Instance) -> None:
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
    surole_password = postgresql_settings.libpq_environ(ctx).get("PGPASSWORD")
    if not surole_password and ctx.settings.postgresql.auth.passfile:
        with pgpass.edit(ctx.settings.postgresql.auth.passfile) as passfile:
            for entry in passfile:
                if entry.matches(port=instance.port, username=surole.name):
                    surole_password = entry.password
    new_manifest = interface.Instance.parse_obj(
        dict(
            _describe(ctx, instance),
            name=name or instance.name,
            version=version,
            port=port or instance.port,
            state=interface.InstanceState.stopped,
            surole_password=SecretStr(surole_password) if surole_password else None,
        )
    )
    init(ctx, new_manifest)
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
    env = postgresql_settings.libpq_environ(ctx)
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


def get_data_checksums(ctx: "BaseContext", instance: system.Instance) -> bool:
    """Return True/False if data_checksums is enabled/disable on instance."""
    if status(ctx, instance) == Status.running:
        # Use SQL SHOW data_checksums since pg_checksums doesn't work if
        # instance is running.
        with db.superuser_connect(ctx, instance) as cnx:
            value = cnx.execute("SHOW data_checksums").fetchall()[0]["data_checksums"]
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
    ctx: "BaseContext", instance: system.Instance, enabled: bool
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


ApplyResult = Union[None, Tuple[system.Instance, ConfigChanges, bool]]


def apply(
    ctx: "BaseContext", manifest: interface.Instance, *, _creating: bool = False
) -> ApplyResult:
    """Apply state described by specified manifest as a PostgreSQL instance.

    Depending on the previous state and existence of the target instance, the
    instance may be created or updated or dropped.

    Unless the target state is 'absent', return an
    :class:`~pglift.model.Instance` object along with configuration changes.

    If configuration changes are detected and the instance was previously
    running, the third item of the return tuple will contain True if the
    server needs to be restarted. If the server needs to be reloaded, this
    will be done automatically.
    """
    States = interface.InstanceState
    state = manifest.state

    if state == States.absent:
        if exists(ctx, manifest.name, manifest.version):
            drop(
                ctx,
                system.Instance.system_lookup(ctx, (manifest.name, manifest.version)),
            )
        return None

    if not exists(ctx, manifest.name, manifest.version):
        _creating = True
        init(ctx, manifest)

    configure_options = manifest.configuration or {}
    configure_options["port"] = manifest.port
    changes = configure(
        ctx,
        manifest,
        ssl=manifest.ssl,
        values=configure_options,
        _creating=_creating,
    )

    instance = system.Instance.system_lookup(ctx, (manifest.name, manifest.version))
    is_running = status(ctx, instance) == Status.running

    if manifest.data_checksums is not None:
        actual_data_checksums = get_data_checksums(ctx, instance)
        if actual_data_checksums != manifest.data_checksums:
            set_data_checksums(ctx, instance, manifest.data_checksums)
            changes["data_checksums"] = (
                "enabled" if actual_data_checksums else "disabled",
                "enabled" if manifest.data_checksums else "disabled",
            )

    needs_restart = False

    if state == States.stopped:
        if is_running:
            stop(ctx, instance)
    elif state == States.started:
        if is_running:
            needs_restart = "port" in changes
            # Check if a restart is needed, unless we're sure it is already
            # (because of 'port' change) and querying for run-time settings
            # would fail.
            if changes and not needs_restart:
                pending_restart = set()
                pending_reload = set()
                for p in settings(ctx, instance):
                    pname = p.name
                    if pname not in changes:
                        continue
                    if p.context == "postmaster":
                        pending_restart.add(pname)
                    elif p.context == "sighup":
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
        else:
            start(ctx, instance)
    else:
        assert False, f"unexpected state: {state}"  # pragma: nocover

    StandbyState = manifest.Standby.State

    if (
        manifest.standby
        and manifest.standby.status == StandbyState.promoted
        and instance.standby is not None
    ):
        promote(ctx, instance)

    if needs_restart and ctx.confirm(
        "Instance needs to be restarted; restart now?", False
    ):
        restart(ctx, instance)
        needs_restart = False

    return instance, changes, needs_restart


def describe(
    ctx: "BaseContext", name: str, version: Optional[str]
) -> interface.Instance:
    """Return an instance described as a manifest."""
    instance = system.Instance.system_lookup(ctx, (name, version))
    is_running = status(ctx, instance) == Status.running
    if not is_running:
        logger.warning(
            "Instance is not running, info about passwords may not be accurate",
        )
    return _describe(ctx, instance)


def _describe(ctx: "BaseContext", instance: system.Instance) -> interface.Instance:
    config = instance.config()
    managed_config = instance.config(managed_only=True).as_dict()
    managed_config.pop("port", None)
    state = interface.InstanceState.from_pg_status(status(ctx, instance))
    services = {
        s.__class__.__service__: s
        for s in ctx.hook.describe(ctx=ctx, instance=instance)
        if s is not None
    }
    if instance.standby:
        standby = interface.Instance.Standby(
            **{"for": instance.standby.for_, "slot": instance.standby.slot}
        )
    else:
        standby = None

    result = interface.Instance(
        name=instance.name,
        version=instance.version,
        port=instance.port,
        state=state,
        ssl=config.get("ssl", False),
        configuration=managed_config,
        standby=standby,
        **services,
    )
    is_running = status(ctx, instance) == Status.running
    if is_running and instance.standby is None:
        surole_name = ctx.settings.postgresql.surole.name
        result.surole_password = roles.describe(ctx, instance, surole_name).password
        replrole = ctx.settings.postgresql.replrole
        result.replrole_password = roles.describe(ctx, instance, replrole).password

    return result


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
    ctx: "BaseContext", *, version: Optional[str] = None
) -> Iterator[interface.InstanceListItem]:
    """Yield instances found by system lookup.

    :param version: filter instances matching a given version.

    :raises ~exceptions.InvalidVersion: if specified version is unknown.
    """
    versions = builtins.list(ctx.settings.postgresql.versions)
    if version:
        if version not in versions:
            raise exceptions.InvalidVersion(f"unknown version '{version}'")
        versions = [version]

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
                instance = system.Instance.system_lookup(ctx, (d.name, ver))
            except exceptions.InstanceNotFound:
                continue

            yield interface.InstanceListItem(
                name=instance.name,
                path=str(instance.path),
                port=instance.port,
                status=status(ctx, instance).name,
                version=instance.version,
            )


def env_for(
    ctx: "BaseContext", instance: system.Instance, *, path: bool = False
) -> Dict[str, str]:
    """Return libpq environment variables suitable to connect to `instance`.

    If 'path' is True, also inject PostgreSQL binaries directory in PATH.
    """
    postgresql_settings = ctx.settings.postgresql
    env = postgresql_settings.libpq_environ(ctx, base={})
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
        }
    )
    if path:
        env["PATH"] = ":".join(
            [str(pg_ctl(instance.version, ctx=ctx).bindir)]
            + ([os.environ["PATH"]] if "PATH" in os.environ else [])
        )
    return env


def exec(
    ctx: "BaseContext", instance: system.Instance, command: Tuple[str, ...]
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


def env(ctx: "BaseContext", instance: system.Instance) -> str:
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
    ctx: "BaseContext", instance: system.Instance
) -> List[interface.PGSetting]:
    """Return the list of run-time parameters of the server, as available in
    pg_settings view.

    The instance must be running.
    """
    with db.superuser_connect(ctx, instance) as cnx, cnx.cursor(
        row_factory=psycopg.rows.class_row(interface.PGSetting)
    ) as cur:
        cur.execute(interface.PGSetting._query)
        return cur.fetchall()


def logs(ctx: "BaseContext", instance: system.Instance) -> Iterator[str]:
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
