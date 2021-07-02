import builtins
import contextlib
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple, Union

from pgtoolkit import conf as pgconf
from pgtoolkit.ctl import Status as Status
from typing_extensions import Literal

from . import (
    conf,
    datapath,
    exceptions,
    hookimpl,
    manifest,
    roles,
    systemd,
    template,
    util,
)
from .ctx import BaseContext
from .model import BaseInstance, Instance, InstanceSpec
from .task import task
from .types import ConfigChanges


def systemd_unit(instance: BaseInstance) -> str:
    """Return systemd unit service name for 'instance'.

    >>> from pglift.settings import Settings
    >>> instance = Instance("test", "13", Settings())
    >>> systemd_unit(instance)
    'postgresql@13-test.service'
    """
    return f"postgresql@{instance.version}-{instance.name}.service"


@task
def init(ctx: BaseContext, instance: InstanceSpec) -> Instance:
    """Initialize a PostgreSQL instance."""
    settings = ctx.settings.postgresql
    initdb_settings = settings.initdb
    surole = settings.surole
    try:
        if instance.exists():
            return Instance.from_spec(instance)
    except LookupError as exc:
        raise Exception(f"instance lookup failed: {exc}")

    # Would raise EnvironmentError if requested postgresql binaries are not
    # available or if versions mismatch.
    pg_ctl = ctx.pg_ctl(instance.version)

    pgroot = settings.root
    pgroot.parent.mkdir(parents=True, exist_ok=True)
    pgroot.mkdir(mode=0o750, exist_ok=True)

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

    if ctx.settings.service_manager == "systemd":
        systemd.enable(ctx, systemd_unit(instance))

    return Instance.from_spec(instance)


@init.revert
def revert_init(ctx: BaseContext, instance: InstanceSpec) -> Any:
    """Un-initialize a PostgreSQL instance."""
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


@task
def configure(
    ctx: BaseContext,
    instance: Instance,
    *,
    ssl: Union[bool, Tuple[Path, Path]] = False,
    **confitems: Any,
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
    if not pgconfig.get("ssl", False):
        if ssl is True:
            util.generate_certificate(configdir, run_command=ctx.run)
        elif isinstance(ssl, tuple):
            try:
                certfile, keyfile = ssl
            except ValueError:
                raise ValueError("expecting a 2-tuple for 'ssl' parameter") from None
            confitems["ssl_cert_file"] = certfile
            confitems["ssl_key_file"] = keyfile
    original_content = postgresql_conf.read_text()
    if not any(line.startswith(include) for line in original_content.splitlines()):
        with postgresql_conf.open("w") as f:
            f.write(f"{include}\n\n")
            f.write(original_content)

    site_confitems: Dict[str, pgconf.Value] = {"cluster_name": instance.name}
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
            try:
                confitems[k] = util.percent_memory(v, memtotal)
            except ValueError:
                pass
        for k, v in confitems.items():
            if isinstance(v, str):
                confitems[k] = v.format(settings=ctx.settings.postgresql)

    format_values(confitems)
    format_values(site_confitems)

    def make_config(fpath: Path, items: Dict[str, Any]) -> ConfigChanges:
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

    i_config = instance.config()
    if "log_directory" in i_config:
        logdir = Path(i_config.log_directory)  # type: ignore[arg-type]
        conf.create_log_directory(instance, logdir)

    ctx.pm.hook.instance_configure(ctx=ctx, instance=instance, changes=changes)

    return changes


@configure.revert
def revert_configure(
    ctx: BaseContext,
    instance: Instance,
    *,
    ssl: Union[bool, Tuple[Path, Path]] = False,
    **kwargs: Any,
) -> Any:
    """Remove custom instance configuration, leaving the default
    'postgresql.conf'.
    """
    i_config = instance.config()
    if "log_directory" in i_config:
        logdir = Path(i_config.log_directory)  # type: ignore[arg-type]
        conf.remove_log_directory(instance, logdir)

    configdir = instance.datadir
    confd, include = conf.info(configdir)
    for name in ("site", "user"):
        conffile = confd / f"{name}.conf"
        if conffile.exists():
            conffile.unlink()
    postgresql_conf = configdir / "postgresql.conf"
    with postgresql_conf.open() as f:
        line = f.readline()
        if line.startswith(include):
            while line:
                # Move to next non-empty line in file.
                pos = f.tell()
                line = f.readline()
                if line.strip():
                    f.seek(pos)
                    break
            rest = f.read()
            with postgresql_conf.open("w") as nf:
                nf.write(rest)
    if ssl is True:
        for ext in ("crt", "key"):
            fpath = configdir / f"server.{ext}"
            if fpath.exists():
                fpath.unlink()


@contextlib.contextmanager
def running(
    ctx: BaseContext,
    instance: Instance,
    *,
    timeout: int = 10,
    run_hooks: bool = False,
) -> Iterator[None]:
    """Context manager to temporarily start an instance.

    :param timeout: delay to wait for instance startup.
    :param run_hooks: whether or not to run hooks during instance start/stop.

    :raises RuntimeError: when the instance did not get through running state
        after specified `timeout` (in seconds).
    """
    if status(ctx, instance) == Status.running:
        yield
        return

    start(ctx, instance, run_hooks=run_hooks)
    for __ in range(timeout):
        time.sleep(1)
        if status(ctx, instance) == Status.running:
            break
    else:
        raise RuntimeError(f"{instance} not started after {timeout}s")
    try:
        yield
    finally:
        stop(ctx, instance, run_hooks=run_hooks)


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: BaseContext, instance: Instance, changes: ConfigChanges
) -> None:
    """Configure authentication for the PostgreSQL instance by setting
    super-user role's password, if any, and installing templated pg_hba.conf
    and pg_ident.conf.

    This is a no-op if if pg_hba.conf's content matches the initial
    configuration.
    """
    surole = ctx.settings.postgresql.surole
    auth_settings = ctx.settings.postgresql.auth
    hba_path = instance.datadir / "pg_hba.conf"
    hba = template("postgresql", "pg_hba.conf").format(
        surole=surole.name,
        auth_local=auth_settings.local,
        auth_host=auth_settings.host,
    )
    if hba_path.read_text() == hba:
        return

    with running(ctx, instance):
        roles.set_password_for(ctx, instance, surole)

    hba_path.write_text(hba)

    ident_path = instance.datadir / "pg_ident.conf"
    ident = template("postgresql", "pg_ident.conf")
    ident_path.write_text(ident)


@task
def start(
    ctx: BaseContext,
    instance: Instance,
    *,
    wait: bool = True,
    logfile: Optional[Path] = None,
    run_hooks: bool = True,
) -> None:
    """Start an instance.

    :param run_hooks: controls whether start-up hook will be triggered or not.
    """
    if ctx.settings.service_manager is None:
        ctx.pg_ctl(instance.version).start(instance.datadir, wait=wait, logfile=logfile)
    elif ctx.settings.service_manager == "systemd":
        systemd.start(ctx, systemd_unit(instance))
    if run_hooks and wait:
        ctx.pm.hook.instance_start(ctx=ctx, instance=instance)


@task
def status(ctx: BaseContext, instance: BaseInstance) -> Status:
    """Return the status of an instance."""
    return ctx.pg_ctl(instance.version).status(instance.datadir)


@task
def stop(
    ctx: BaseContext,
    instance: Instance,
    *,
    mode: str = "fast",
    wait: bool = True,
    run_hooks: bool = False,
) -> None:
    """Stop an instance.

    :param run_hooks: controls whether stop hook will be triggered or not.
    """
    if ctx.settings.service_manager is None:
        ctx.pg_ctl(instance.version).stop(instance.datadir, mode=mode, wait=wait)
    elif ctx.settings.service_manager == "systemd":
        systemd.stop(ctx, systemd_unit(instance))
    if run_hooks and wait:
        ctx.pm.hook.instance_stop(ctx=ctx, instance=instance)


@task
def restart(
    ctx: BaseContext,
    instance: Instance,
    *,
    mode: str = "fast",
    wait: bool = True,
) -> None:
    """Restart an instance."""
    if ctx.settings.service_manager is None:
        ctx.pg_ctl(instance.version).restart(instance.datadir, mode=mode, wait=wait)
    elif ctx.settings.service_manager == "systemd":
        systemd.restart(ctx, systemd_unit(instance))


@task
def reload(
    ctx: BaseContext,
    instance: Instance,
) -> None:
    """Reload an instance."""
    if ctx.settings.service_manager is None:
        ctx.pg_ctl(instance.version).reload(instance.datadir)
    elif ctx.settings.service_manager == "systemd":
        systemd.reload(ctx, systemd_unit(instance))


def apply(ctx: BaseContext, instance_manifest: manifest.Instance) -> Optional[Instance]:
    """Apply state described by specified manifest as a PostgreSQL instance.

    Depending on the previous state and existence of the target instance, the
    instance may be created or updated or dropped.

    Unless the target state is 'absent' an :class:`~pglift.model.Instance`
    object is returned.

    If configuration changes are detected and the instance was previously
    running, it will be reloaded. Note that some changes require a full
    restart, this needs to be handled manually.
    """
    instance_spec = instance_manifest.model(ctx)
    States = manifest.InstanceState
    state = instance_manifest.state

    if state == States.absent:
        if instance_spec.exists():
            instance = Instance.from_spec(instance_spec)
            drop(ctx, instance)
        return None

    if not instance_spec.exists():
        instance = init(ctx, instance_spec)
    else:
        instance = Instance.from_spec(instance_spec)
    configure_options = instance_manifest.configuration or {}
    changes = configure(
        ctx,
        instance,
        ssl=instance_manifest.ssl,
        port=instance_manifest.port,
        **configure_options,
    )

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

    return instance


def describe(ctx: BaseContext, instance: Instance) -> manifest.Instance:
    """Return an instance described as a manifest."""
    config = instance.config()
    managed_config = instance.config(managed_only=True).as_dict()
    managed_config.pop("port", None)
    state = manifest.InstanceState.from_pg_status(status(ctx, instance))
    return manifest.Instance(
        name=instance.name,
        version=instance.version,
        port=instance.port,
        state=state,
        ssl=config.ssl,
        configuration=managed_config,
    )


def drop(ctx: BaseContext, instance: Instance) -> None:
    """Drop an instance."""
    ctx.pm.hook.instance_drop(ctx=ctx, instance=instance)

    revert_configure(ctx, instance)
    revert_init(ctx, instance.as_spec())


def list(
    ctx: BaseContext, *, version: Optional[str] = None
) -> Iterator[manifest.InstanceListItem]:
    """Yield instances found by system lookup.

    :param version: filter instances matching a given version.
    """
    versions = builtins.list(ctx.settings.postgresql.versions)
    if version:
        if version not in versions:
            raise ValueError(f"unknown version '{version}'")
        versions = [version]

    pgroot = ctx.settings.postgresql.root
    assert pgroot.is_dir(), f"{pgroot} isn't a directory"
    # Search for directories looking like <version>/<name> in pgroot
    for ver in versions:
        version_path = pgroot / ver
        if not version_path.is_dir():
            continue
        for d in version_path.iterdir():
            if not d.is_dir():
                continue
            instance_spec = InstanceSpec(d.name, ver, settings=ctx.settings)
            try:
                instance = Instance.from_spec(instance_spec)
            except exceptions.InstanceNotFound:
                continue

            yield manifest.InstanceListItem(
                name=instance.name,
                path=str(instance.path),
                port=instance.port,
                status=status(ctx, instance).name,
                version=instance.version,
            )
