import functools
import subprocess
from pathlib import Path
from typing import Callable

from . import exceptions
from . import template as _template
from .ctx import BaseContext
from .types import Logger


def template(name: str) -> str:
    return _template("systemd", name)


def install(name: str, content: str, unit_path: Path, *, logger: Logger) -> None:
    path = unit_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_text() != content:
        if path.exists():
            raise exceptions.FileExistsError(f"{path} exists, not overwriting")
        path.write_text(content)
        logger.info("installed %s systemd unit at %s", name, path)


def uninstall(name: str, unit_path: Path, *, logger: Logger) -> None:
    path = unit_path / name
    if path.exists():
        path.unlink()
        logger.info("removed %s systemd unit (%s)", name, path)


def daemon_reload(ctx: BaseContext) -> None:
    ctx.run(["systemctl", "--user", "daemon-reload"], check=True)


def is_enabled(ctx: BaseContext, unit: str) -> bool:
    r = ctx.run(["systemctl", "--quiet", "--user", "is-enabled", unit], check=False)
    return r.returncode == 0


def enable(ctx: BaseContext, unit: str, *, now: bool = False) -> None:
    if is_enabled(ctx, unit):
        ctx.debug("systemd unit %s already enabled, 'enable' action skipped", unit)
        return
    cmd = ["systemctl", "--user", "enable", unit]
    if now:
        cmd.append("--now")
    ctx.run(cmd, check=True)


def disable(ctx: BaseContext, unit: str, *, now: bool = True) -> None:
    if not is_enabled(ctx, unit):
        ctx.debug("systemd unit %s not enabled, 'disable' action skipped", unit)
        return
    cmd = ["systemctl", "--user", "disable", unit]
    if now:
        cmd.append("--now")
    ctx.run(cmd, check=True)


F = Callable[[BaseContext, str], None]


def log_status(fn: F) -> F:
    @functools.wraps(fn)
    def wrapper(ctx: BaseContext, unit: str) -> None:
        try:
            return fn(ctx, unit)
        except (subprocess.CalledProcessError, SystemExit):
            # Ansible runner would call sys.exit(1), hence SystemExit.
            ctx.error(status(ctx, unit))
            raise

    return wrapper


def status(ctx: BaseContext, unit: str, *, full: bool = True) -> str:
    opts = []
    if full:
        opts.append("--full")
    return ctx.run(["systemctl", "--user"] + opts + ["status", unit], check=True).stdout


@log_status
def start(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "start", unit], check=True)


@log_status
def stop(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "stop", unit], check=True)


@log_status
def reload(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "reload", unit], check=True)


@log_status
def restart(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "restart", unit], check=True)


def is_active(ctx: BaseContext, unit: str) -> bool:
    r = ctx.run(["systemctl", "--quiet", "--user", "is-active", unit], check=False)
    return r.returncode == 0
