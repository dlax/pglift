import contextlib
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple, Union

import psycopg2
from pgtoolkit import conf as pgconf
from pgtoolkit.ctl import Status as Status
from typing_extensions import Literal

from . import conf, manifest, queries, systemd, template, util
from .ctx import BaseContext, Context
from .model import Instance
from .task import runner, task


def systemd_unit(instance: Instance) -> str:
    """Return systemd unit service name for 'instance'.

    >>> instance = Instance("test", "13")
    >>> systemd_unit(instance)
    'postgresql@13-test.service'
    """
    return f"postgresql@{instance.version}-{instance.name}.service"


@task
def init(ctx: BaseContext, instance: Instance) -> bool:
    """Initialize a PostgreSQL instance."""
    settings = ctx.settings.postgresql
    initdb_settings = settings.initdb
    surole = settings.surole
    try:
        if instance.exists():
            return False
    except Exception as exc:
        raise Exception(f"instance lookup failed: {exc}")

    # Would raise EnvironmentError if requested postgresql binaries are not
    # available or if versions mismatch.
    pg_ctl = ctx.pg_ctl(instance.version)

    pgroot = settings.root
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

    return True


@init.revert
def revert_init(ctx: BaseContext, instance: Instance) -> Any:
    """Un-initialize a PostgreSQL instance."""
    if ctx.settings.service_manager == "systemd":
        unit = systemd_unit(instance)
        if systemd.is_enabled(ctx, unit):
            systemd.disable(ctx, unit, now=True)

    settings = ctx.settings.postgresql
    shutil.rmtree(instance.path)
    pgroot = settings.root
    try:
        next(pgroot.iterdir())
    except StopIteration:
        # directory is empty
        pgroot.rmdir()


ConfigChanges = Dict[str, Tuple[Optional[pgconf.Value], Optional[pgconf.Value]]]


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
    """
    configdir = instance.datadir
    postgresql_conf = configdir / "postgresql.conf"
    assert postgresql_conf.exists()
    our_confd, our_conffile, include = conf.info(configdir)
    if not our_confd.exists():
        our_confd.mkdir()
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

    config = conf.make(instance.name, **confitems)

    config_before = {}
    if our_conffile.exists():
        config_before = {e.name: e.value for e in pgconf.parse(our_conffile)}
    config_after = {e.name: e.value for e in config}
    changes: ConfigChanges = {}
    for k in set(config_before) | set(config_after):
        pv = config_before.get(k)
        nv = config_after.get(k)
        if nv != pv:
            changes[k] = (pv, nv)

    if changes:
        with our_conffile.open("w") as f:
            config.save(f)

    ctx.pm.hook.instance_configure(ctx=ctx, instance=instance)

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
    configdir = instance.datadir
    our_confd, our_conffile, include = conf.info(configdir)
    if our_conffile.exists():
        our_conffile.unlink()
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


@task
def configure_auth(ctx: BaseContext, instance: Instance) -> None:
    """Configure authentication for the PostgreSQL instance by setting
    super-user role's password, if any, and installing templated pg_hba.conf.
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

    if surole.password is not None:
        config = instance.config()
        assert config is not None
        password = surole.password.get_secret_value()
        connargs = {
            "port": config.port,
            "dbname": "postgres",
            "user": surole.name,
        }
        if config.unix_socket_directories:
            connargs["host"] = config.unix_socket_directories
        with running(ctx, instance):
            with psycopg2.connect(**connargs) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(
                        queries.get("role_alter_password", username=surole.name),
                        {"password": password},
                    )

    hba_path.write_text(hba)


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
def status(
    ctx: BaseContext,
    instance: Instance,
) -> Status:
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


def apply(ctx: BaseContext, instance_manifest: manifest.Instance) -> None:
    """Apply state described by specified manifest as a PostgreSQL instance.

    Depending on the previous state and existence of the target instance, the
    instance may be created or updated or dropped.

    If configuration changes are detected and the instance was previously
    running, it will be reloaded. Note that some changes require a full
    restart, this needs to be handled manually.
    """
    instance = instance_manifest.model(ctx)
    States = manifest.InstanceState
    state = instance_manifest.state

    if state == States.absent:
        if instance.exists():
            drop(ctx, instance)
        return

    if not instance.exists():
        init(ctx, instance)
    configure_options = instance_manifest.configuration or {}
    changes = configure(
        ctx,
        instance,
        ssl=instance_manifest.ssl,
        **configure_options,
    )
    configure_auth(ctx, instance)

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


def describe(ctx: BaseContext, instance: Instance) -> Optional[manifest.Instance]:
    """Return an instance described as a manifest (or None if the instance
    does not exists).
    """
    if not instance.exists():
        return None
    config = instance.config()
    assert config
    managed_config = instance.config(managed_only=True)
    assert managed_config
    state = manifest.InstanceState.from_pg_status(status(ctx, instance))
    return manifest.Instance(
        name=instance.name,
        state=state,
        version=instance.version,
        ssl=config.ssl,
        configuration=managed_config.as_dict(),
    )


def drop(
    ctx: BaseContext,
    instance: Instance,
) -> None:
    """Drop an instance."""
    if not instance.exists():
        return

    ctx.pm.hook.instance_drop(ctx=ctx, instance=instance)

    revert_configure(ctx, instance)
    revert_init(ctx, instance)


if __name__ == "__main__":  # pragma: nocover
    import argparse

    from . import pm
    from .settings import SETTINGS

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    def instance_subparser(*args: Any, **kwargs: Any) -> argparse.ArgumentParser:
        subparser = subparsers.add_parser(*args, **kwargs)
        subparser.add_argument("--name", required=True)
        subparser.add_argument("--version", required=False)
        return subparser

    def get_instance(ctx: BaseContext, args: argparse.Namespace) -> Instance:
        if args.version:
            return Instance(args.name, args.version)
        else:
            return Instance.default_version(args.name, ctx)

    apply_parser = subparsers.add_parser(
        "apply",
        help="apply manifest as a PostgreSQL instance",
    )
    apply_parser.add_argument(
        "-f", "--file", type=argparse.FileType(), metavar="MANIFEST", required=True
    )

    def do_apply(ctx: BaseContext, args: argparse.Namespace) -> None:
        with runner():
            apply(ctx, manifest.Instance.parse_yaml(args.file))

    apply_parser.set_defaults(func=do_apply)

    schema_parser = subparsers.add_parser(
        "schema",
        help="print the JSON schema of PostgreSQL instance model",
    )

    def do_schema(ctx: BaseContext, args: argparse.Namespace) -> None:
        print(manifest.Instance.schema_json(indent=2))

    schema_parser.set_defaults(func=do_schema)

    describe_parser = instance_subparser(
        "describe",
        help="describe a PostgreSQL instance",
    )

    def do_describe(ctx: BaseContext, args: argparse.Namespace) -> None:
        instance = get_instance(ctx, args)
        described = describe(ctx, instance)
        if described:
            print(described.yaml(), end="")

    describe_parser.set_defaults(func=do_describe)

    drop_parser = instance_subparser(
        "drop",
        help="drop a PostgreSQL instance",
    )

    def do_drop(ctx: BaseContext, args: argparse.Namespace) -> None:
        instance = get_instance(ctx, args)
        drop(ctx, instance)

    drop_parser.set_defaults(func=do_drop)

    args = parser.parse_args()
    ctx = Context(plugin_manager=pm.PluginManager.get(), settings=SETTINGS)
    args.func(ctx, args)
