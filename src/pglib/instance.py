from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from pgtoolkit import conf as pgconf
from pgtoolkit.ctl import Status as Status
from typing_extensions import Literal

from . import conf, util
from .ctx import BaseContext
from .model import Instance
from .settings import SETTINGS, PostgreSQLSettings
from .task import task
from .util import short_version

POSTGRESQL_SETTINGS = SETTINGS.postgresql


@task
def init(
    ctx: BaseContext,
    instance: Instance,
    *,
    data_checksums: bool = False,
    settings: PostgreSQLSettings = POSTGRESQL_SETTINGS,
) -> bool:
    """Initialize a PostgreSQL instance."""
    try:
        if instance.exists():
            return False
    except Exception as exc:
        raise Exception(f"instance lookup failed: {exc}")

    # Check if the version provided matches the version installed
    installed_version = short_version(ctx.pg_ctl.version)
    if installed_version != instance.version:
        raise Exception(
            f"version doesn't match installed version {instance.version} != {installed_version}"
        )

    pgroot = settings.root
    pgroot.mkdir(mode=0o750, exist_ok=True)

    opts: Dict[str, Union[str, Literal[True]]] = {
        "waldir": str(instance.waldir),
        "username": settings.surole,
        "encoding": "UTF8",
    }
    if settings.locale:
        opts["locale"] = settings.locale
    if data_checksums:
        opts["data_checksums"] = True
    ctx.pg_ctl.init(instance.datadir, **opts)
    return True


@init.revert
def revert_init(
    ctx: BaseContext,
    instance: Instance,
    *,
    settings: PostgreSQLSettings = POSTGRESQL_SETTINGS,
    **kwargs: Any,
) -> Any:
    """Un-initialize a PostgreSQL instance."""
    ctx.run(["rm", "-rf", str(instance.path)], check=True)
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
    settings: PostgreSQLSettings = POSTGRESQL_SETTINGS,
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

    return changes


@configure.revert
def revert_configure(
    ctx: BaseContext,
    instance: Instance,
    *,
    ssl: Union[bool, Tuple[Path, Path]] = False,
    settings: PostgreSQLSettings = POSTGRESQL_SETTINGS,
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


@task
def start(
    ctx: BaseContext,
    instance: Instance,
    *,
    wait: bool = True,
    logfile: Optional[Path] = None,
) -> None:
    """Start an instance."""
    ctx.pg_ctl.start(instance.datadir, wait=wait, logfile=logfile)


@task
def status(
    ctx: BaseContext,
    instance: Instance,
) -> Status:
    """Return the status of an instance."""
    return ctx.pg_ctl.status(instance.datadir)


@task
def stop(
    ctx: BaseContext,
    instance: Instance,
    *,
    mode: str = "fast",
    wait: bool = True,
) -> None:
    """Stop an instance."""
    ctx.pg_ctl.stop(instance.datadir, mode=mode, wait=wait)


@task
def restart(
    ctx: BaseContext,
    instance: Instance,
    *,
    mode: str = "fast",
    wait: bool = True,
) -> None:
    """Restart an instance."""
    ctx.pg_ctl.restart(instance.datadir, mode=mode, wait=wait)


@task
def reload(
    ctx: BaseContext,
    instance: Instance,
) -> None:
    """Reload an instance."""
    ctx.pg_ctl.reload(instance.datadir)
