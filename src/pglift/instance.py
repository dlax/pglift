import builtins
import contextlib
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple, Union

from pgtoolkit import conf as pgconf
from pgtoolkit import pgpass
from pgtoolkit.ctl import Status as Status
from pydantic import SecretStr
from typing_extensions import Literal

from . import (
    cmd,
    conf,
    datapath,
    db,
    exceptions,
    hookimpl,
    roles,
    systemd,
    template,
    util,
)
from .ctx import BaseContext
from .models import interface
from .models.system import (
    BaseInstance,
    Instance,
    PostgreSQLInstance,
    default_postgresql_version,
)
from .task import task
from .types import ConfigChanges


def systemd_unit(instance: BaseInstance) -> str:
    return f"pglift-postgresql@{instance.version}-{instance.name}.service"


def init_replication(
    ctx: BaseContext, instance: BaseInstance, standby_for: str, slot: Optional[str]
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
        bindir = ctx.pg_ctl(instance.version).bindir
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

        env = ctx.libpq_environ()
        if slot:
            cmd += ["--slot", slot]
            with db.connect_dsn(
                standby_for,
                passfile=env.get("PGPASSFILE"),
                password=env.get("PGPASSWORD"),
            ) as cnx:
                # ensure the replication slot does not exists
                # otherwise --create-slot will raise an error
                with cnx.cursor() as cur:
                    cur.execute(db.query("drop_replication_slot"), {"slot": slot})
                    if int(instance.version) <= 10:
                        cur.execute(db.query("create_replication_slot"), {"slot": slot})
                    else:
                        cmd += ["--create-slot"]

        ctx.run(cmd, env=env, check=True)
        for name in keep:
            shutil.copyfile(tmpdir / name, instance.datadir / name)
        # When primary is also managed by pglift, pg_basebackup will also copy
        # conf.pglift.d. So we must drop it to not interfer with site/user
        # config generated by instance_configure
        shutil.rmtree(instance.datadir / "conf.pglift.d", ignore_errors=True)


@task("initialize PostgreSQL instance")
def init(ctx: BaseContext, manifest: interface.Instance) -> None:
    """Initialize a PostgreSQL instance."""
    settings = ctx.settings.postgresql
    initdb_settings = settings.initdb
    surole = settings.surole

    if exists(ctx, manifest.name, manifest.version):
        return None

    instance = BaseInstance.get(manifest.name, manifest.version, ctx)

    # Would raise SystemError if requested postgresql binaries are not
    # available or if versions mismatch.
    pg_ctl = ctx.pg_ctl(manifest.version)

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
    if initdb_settings.data_checksums:
        opts["data_checksums"] = True

    pg_ctl.init(instance.datadir, **opts)
    if manifest.standby:
        init_replication(ctx, instance, manifest.standby.for_, manifest.standby.slot)

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(instance))

    return None


@init.revert("delete PostgreSQL instance")
def revert_init(ctx: BaseContext, manifest: interface.Instance) -> None:
    """Un-initialize a PostgreSQL instance."""
    instance = BaseInstance.get(manifest.name, manifest.version, ctx)
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


def configure(
    ctx: BaseContext,
    manifest: interface.Instance,
    *,
    ssl: Union[bool, Tuple[Path, Path]] = False,
    **confitems: Optional[pgconf.Value],
) -> ConfigChanges:
    """Write instance's configuration and include it in its postgresql.conf.

    `ssl` parameter controls SSL configuration. If False, SSL is not enabled.
    If True, a self-signed certificate is generated. A tuple of two
    `~pathlib.Path` corresponding to the location of SSL cert file and key
    file to use may also be passed.

    'shared_buffers' and 'effective_cache_size' setting, if defined and set to
    a percent-value, will be converted to proper memory value relative to the
    total memory available on the system.
    """
    instance = PostgreSQLInstance.system_lookup(ctx, (manifest.name, manifest.version))
    configdir = instance.datadir
    postgresql_conf = configdir / "postgresql.conf"
    confd, include = conf.info(configdir)
    if not confd.exists():
        confd.mkdir()
    site_conffile = confd / "site.conf"
    user_conffile = confd / "user.conf"
    pgconfig = pgconf.parse(str(postgresql_conf))
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
    site_config_template = datapath / "postgresql" / "site.conf"
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
    ) -> ConfigChanges:
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

        if changes:
            with fpath.open("w") as f:
                config.save(f)

        return changes

    make_config(site_conffile, site_confitems)
    changes = make_config(user_conffile, confitems)

    i = PostgreSQLInstance.system_lookup(ctx, instance)
    i_config = i.config()
    ctx.pm.hook.instance_configure(
        ctx=ctx, manifest=manifest, config=i_config, changes=changes
    )

    if "log_directory" in i_config:
        logdir = Path(i_config.log_directory)  # type: ignore[arg-type]
        conf.create_log_directory(instance, logdir)

    return changes


@contextlib.contextmanager
def running(
    ctx: BaseContext,
    instance: Union[PostgreSQLInstance, Instance],
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

    if run_hooks and not isinstance(instance, Instance):
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
    ctx: BaseContext,
    instance: Union[PostgreSQLInstance, Instance],
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

    if run_hooks and not isinstance(instance, Instance):
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
def instance_configure(
    ctx: BaseContext, manifest: interface.Instance, **kwargs: Any
) -> None:
    """Configure authentication for the PostgreSQL instance by setting
    super-user role's password, if any, and installing templated pg_hba.conf
    and pg_ident.conf.

    This is a no-op if if pg_hba.conf's content matches the initial
    configuration.
    """
    surole = interface.instance_surole(ctx.settings, manifest)
    auth_settings = ctx.settings.postgresql.auth
    instance = Instance.system_lookup(ctx, (manifest.name, manifest.version))
    hba_path = instance.datadir / "pg_hba.conf"
    hba = template("postgresql", "pg_hba.conf").format(
        surole=surole.name,
        auth_local=auth_settings.local,
        auth_host=auth_settings.host,
    )
    if hba_path.read_text() == hba:
        return

    if not instance.standby and surole.password:
        # standby instances are read-only
        with running(ctx, instance):
            roles.set_password_for(ctx, instance, surole)

    hba_path.write_text(hba)

    ident_path = instance.datadir / "pg_ident.conf"
    ident = template("postgresql", "pg_ident.conf")
    ident_path.write_text(ident)


def start(
    ctx: BaseContext,
    instance: Union[PostgreSQLInstance, Instance],
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
    if run_hooks and not isinstance(instance, Instance):
        raise TypeError("expecting a full instance")

    ctx.info("starting instance %s", instance)
    if foreground and run_hooks:
        ctx.debug("not running hooks for a foreground start")
        run_hooks = False

    start_postgresql(ctx, instance, wait=wait, logfile=logfile, foreground=foreground)

    if run_hooks and wait:
        ctx.pm.hook.instance_start(ctx=ctx, instance=instance)


@task("start PostgreSQL instance")
def start_postgresql(
    ctx: BaseContext,
    instance: Union[PostgreSQLInstance, Instance],
    *,
    wait: bool = True,
    logfile: Optional[Path] = None,
    foreground: bool = False,
) -> None:
    if ctx.settings.service_manager is None:
        if foreground:
            postgres = ctx.pg_ctl(instance.version).bindir / "postgres"
            cmd.execute_program(
                [str(postgres), "-D", str(instance.datadir)], logger=ctx
            )
        else:
            ctx.pg_ctl(instance.version).start(
                instance.datadir, wait=wait, logfile=logfile
            )
    elif ctx.settings.service_manager == "systemd":
        systemd.start(ctx, systemd_unit(instance))


@task("get PostgreSQL instance status")
def status(ctx: BaseContext, instance: BaseInstance) -> Status:
    """Return the status of an instance."""
    return ctx.pg_ctl(instance.version).status(instance.datadir)


def check_status(ctx: BaseContext, instance: BaseInstance, expected: Status) -> None:
    """Check actual instance status with respected to `expected` one.

    :raises ~exceptions.InstanceStateError: in case the actual status is not expected.
    """
    st = status(ctx, instance)
    if st != expected:
        raise exceptions.InstanceStateError(f"instance is {st.name}")


def stop(
    ctx: BaseContext,
    instance: Union[PostgreSQLInstance, Instance],
    *,
    mode: str = "fast",
    wait: bool = True,
    run_hooks: bool = True,
) -> None:
    """Stop an instance.

    :param run_hooks: controls whether stop hook will be triggered or not.
    """
    if run_hooks and not isinstance(instance, Instance):
        raise TypeError("expecting a full instance")

    if status(ctx, instance) == Status.not_running:
        ctx.warning("instance %s is already stopped", instance)
    else:
        stop_postgresql(ctx, instance, mode=mode, wait=wait)
    if run_hooks and wait:
        ctx.pm.hook.instance_stop(ctx=ctx, instance=instance)


@task("stop PostgreSQL instance")
def stop_postgresql(
    ctx: BaseContext,
    instance: Union[PostgreSQLInstance, Instance],
    mode: str = "fast",
    wait: bool = True,
) -> None:
    ctx.info("stopping instance %s", instance)
    if ctx.settings.service_manager is None:
        ctx.pg_ctl(instance.version).stop(instance.datadir, mode=mode, wait=wait)
    elif ctx.settings.service_manager == "systemd":
        systemd.stop(ctx, systemd_unit(instance))


@task("restart PostgreSQL instance")
def restart(
    ctx: BaseContext,
    instance: Instance,
    *,
    mode: str = "fast",
    wait: bool = True,
) -> None:
    """Restart an instance."""
    ctx.info("restarting instance %s", instance)
    if ctx.settings.service_manager is None:
        ctx.pg_ctl(instance.version).restart(instance.datadir, mode=mode, wait=wait)
    elif ctx.settings.service_manager == "systemd":
        systemd.restart(ctx, systemd_unit(instance))


@task("reload PostgreSQL instance")
def reload(
    ctx: BaseContext,
    instance: Instance,
) -> None:
    """Reload an instance."""
    ctx.info("reloading instance %s", instance)
    if ctx.settings.service_manager is None:
        ctx.pg_ctl(instance.version).reload(instance.datadir)
    elif ctx.settings.service_manager == "systemd":
        systemd.reload(ctx, systemd_unit(instance))


def promote(ctx: BaseContext, instance: Instance) -> None:
    """Promote a standby instance"""
    pg_ctl = ctx.pg_ctl(instance.version)
    pg_ctl.run_command(
        [str(pg_ctl.pg_ctl), "promote", "-D", str(instance.datadir)],
        check=True,
    )


def upgrade(
    ctx: BaseContext,
    instance: Instance,
    *,
    version: Optional[str] = None,
    name: Optional[str] = None,
    port: Optional[int] = None,
    jobs: Optional[int] = None,
) -> Instance:
    """Upgrade an instance using pg_upgrade"""
    if version is None:
        version = default_postgresql_version(ctx)
    surole = ctx.settings.postgresql.surole
    surole_password = ctx.libpq_environ().get("PGPASSWORD")
    if not surole_password and ctx.settings.postgresql.auth.passfile:
        with pgpass.edit(ctx.settings.postgresql.auth.passfile) as passfile:
            for entry in passfile:
                if entry.matches(port=instance.port, username=surole.name):
                    surole_password = entry.password
    new_manifest = interface.Instance(
        name=name or instance.name,
        version=version,
        port=port or instance.port,
        state=interface.InstanceState.stopped,
        prometheus={"port": instance.prometheus.port},
        surole_password=SecretStr(surole_password) if surole_password else None,
    )
    result = apply(ctx, new_manifest)
    assert result is not None, new_manifest
    (newinstance, _) = result
    bindir = ctx.pg_ctl(version).bindir
    pg_upgrade = str(bindir / "pg_upgrade")
    cmd = [
        pg_upgrade,
        f"--old-bindir={ctx.pg_ctl(instance.version).bindir}",
        f"--new-bindir={bindir}",
        f"--old-datadir={instance.datadir}",
        f"--new-datadir={newinstance.datadir}",
        f"--username={ctx.settings.postgresql.surole.name}",
    ]
    if jobs is not None:
        cmd.extend(["--jobs", str(jobs)])
    env = ctx.libpq_environ()
    if surole_password:
        env.setdefault("PGPASSWORD", surole_password)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            ctx.run(cmd, check=True, cwd=tmpdir, env=env)
    except exceptions.CommandError:
        drop(ctx, newinstance)
        raise
    return newinstance


def apply(
    ctx: BaseContext, manifest: interface.Instance
) -> Optional[Tuple[Instance, ConfigChanges]]:
    """Apply state described by specified manifest as a PostgreSQL instance.

    Depending on the previous state and existence of the target instance, the
    instance may be created or updated or dropped.

    Unless the target state is 'absent', return an
    :class:`~pglift.model.Instance` object along with configuration changes.

    If configuration changes are detected and the instance was previously
    running, it will be reloaded. Note that some changes require a full
    restart, this needs to be handled manually.
    """
    States = interface.InstanceState
    state = manifest.state

    if state == States.absent:
        if exists(ctx, manifest.name, manifest.version):
            drop(ctx, Instance.system_lookup(ctx, (manifest.name, manifest.version)))
        return None

    if not exists(ctx, manifest.name, manifest.version):
        init(ctx, manifest)

    configure_options = manifest.configuration or {}
    changes = configure(
        ctx,
        manifest,
        ssl=manifest.ssl,
        port=manifest.port,
        **configure_options,
    )

    instance = Instance.system_lookup(ctx, (manifest.name, manifest.version))
    is_running = status(ctx, instance) == Status.running
    if state == States.stopped:
        if is_running:
            stop(ctx, instance)
    elif state == States.started:
        if is_running:
            if changes:
                # This might fail because some changes require a restart but
                # 'pg_ctl reload' does not inform us about that.
                reload(ctx, instance)
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

    return instance, changes


def describe(ctx: BaseContext, name: str, version: Optional[str]) -> interface.Instance:
    """Return an instance described as a manifest."""
    instance = Instance.system_lookup(ctx, (name, version))
    config = instance.config()
    managed_config = instance.config(managed_only=True).as_dict()
    managed_config.pop("port", None)
    state = interface.InstanceState.from_pg_status(status(ctx, instance))
    prometheus = interface.Instance.Prometheus(port=instance.prometheus.port)
    return interface.Instance(
        name=instance.name,
        version=instance.version,
        port=instance.port,
        state=state,
        ssl=config.ssl,
        configuration=managed_config,
        prometheus=prometheus,
    )


@task("drop PostgreSQL instance")
def drop(ctx: BaseContext, instance: Instance) -> None:
    """Drop an instance."""
    stop(ctx, instance, run_hooks=True)

    ctx.pm.hook.instance_drop(ctx=ctx, instance=instance)
    manifest = interface.Instance(name=instance.name, version=instance.version)
    revert_init(ctx, manifest)


def list(
    ctx: BaseContext, *, version: Optional[str] = None
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
                instance = Instance.system_lookup(ctx, (d.name, ver))
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
    ctx: BaseContext, instance: Instance, *, path: bool = False
) -> Dict[str, str]:
    """Return libpq environment variables suitable to connect to `instance`.

    If 'path' is True, also inject PostgreSQL binaries directory in PATH.
    """
    env = ctx.libpq_environ(base={})
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
        }
    )
    if path:
        env["PATH"] = ":".join(
            [str(ctx.pg_ctl(instance.version).bindir)]
            + ([os.environ["PATH"]] if "PATH" in os.environ else [])
        )
    return env


def exec(ctx: BaseContext, instance: Instance, command: Tuple[str, ...]) -> None:
    """Execute given PostgreSQL command in the libpq environment for `instance`.

    The command to be executed is looked up for in PostgreSQL binaries directory.
    """
    env = os.environ.copy()
    env.update(env_for(ctx, instance))
    progname, *args = command
    program = ctx.pg_ctl(instance.version).bindir / progname
    try:
        cmd.execute_program([str(program)] + args, env=env)
    except FileNotFoundError as e:
        raise exceptions.FileNotFoundError(str(e))


def env(ctx: BaseContext, instance: Instance) -> str:
    return "\n".join(
        [
            f"export {key}={value}"
            for key, value in sorted(env_for(ctx, instance, path=True).items())
        ]
    )


def exists(ctx: BaseContext, name: str, version: Optional[str]) -> bool:
    """Return true when instance exists"""
    try:
        PostgreSQLInstance.system_lookup(ctx, (name, version))
    except exceptions.InstanceNotFound:
        return False
    return True
