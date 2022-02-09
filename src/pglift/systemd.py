import functools
import logging
import subprocess
from logging import Logger
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List

from . import exceptions, util
from .settings import Settings, SystemdSettings

if TYPE_CHECKING:
    from .ctx import BaseContext

logger = logging.getLogger(__name__)


def template(name: str) -> str:
    return util.template("systemd", name)


def executeas(settings: Settings) -> str:
    """Return User/Group options for systemd unit depending on settings.

    When systemd.user is set, return an empty string:
    >>> s = Settings.parse_obj({"systemd": {"user": True}})
    >>> executeas(s)
    ''

    Otherwise, user sysuser setting:
    >>> s = Settings.parse_obj({"systemd": {"user": False}, "sysuser": ["postgres", "pgsql"]})
    >>> print(executeas(s))
    User=postgres
    Group=pgsql
    """
    if settings.systemd.user:
        return ""
    user, group = settings.sysuser
    return "\n".join([f"User={user}", f"Group={group}"])


def systemctl(settings: SystemdSettings, *args: str) -> List[str]:
    sflag = "--user" if settings.user else "--system"
    cmd = ["systemctl", sflag] + list(args)
    if settings.sudo:
        cmd.insert(0, "sudo")
    return cmd


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


def daemon_reload(ctx: "BaseContext") -> None:
    ctx.run(systemctl(ctx.settings.systemd, "daemon-reload"), check=True)


def is_enabled(ctx: "BaseContext", unit: str) -> bool:
    r = ctx.run(
        systemctl(ctx.settings.systemd, "--quiet", "is-enabled", unit),
        check=False,
    )
    return r.returncode == 0


def enable(ctx: "BaseContext", unit: str, *, now: bool = False) -> None:
    if is_enabled(ctx, unit):
        logger.debug("systemd unit %s already enabled, 'enable' action skipped", unit)
        return
    cmd = systemctl(ctx.settings.systemd, "enable", unit)
    if now:
        cmd.append("--now")
    ctx.run(cmd, check=True)


def disable(ctx: "BaseContext", unit: str, *, now: bool = True) -> None:
    if not is_enabled(ctx, unit):
        logger.debug("systemd unit %s not enabled, 'disable' action skipped", unit)
        return
    cmd = systemctl(ctx.settings.systemd, "disable", unit)
    if now:
        cmd.append("--now")
    ctx.run(cmd, check=True)


F = Callable[["BaseContext", str], None]


def log_status(fn: F) -> F:
    @functools.wraps(fn)
    def wrapper(ctx: "BaseContext", unit: str) -> None:
        try:
            return fn(ctx, unit)
        except (subprocess.CalledProcessError, SystemExit):
            # Ansible runner would call sys.exit(1), hence SystemExit.
            logger.error(status(ctx, unit))
            raise

    return wrapper


def status(ctx: "BaseContext", unit: str) -> str:
    proc = ctx.run(
        systemctl(ctx.settings.systemd, "--full", "--lines=100", "status", unit),
        check=False,
    )
    # https://www.freedesktop.org/software/systemd/man/systemctl.html#Exit%20status
    if proc.returncode not in (0, 1, 2, 3, 4):
        raise exceptions.CommandError(
            proc.returncode, proc.args, proc.stdout, proc.stderr
        )
    return proc.stdout


@log_status
def start(ctx: "BaseContext", unit: str) -> None:
    ctx.run(systemctl(ctx.settings.systemd, "start", unit), check=True)


@log_status
def stop(ctx: "BaseContext", unit: str) -> None:
    ctx.run(systemctl(ctx.settings.systemd, "stop", unit), check=True)


@log_status
def reload(ctx: "BaseContext", unit: str) -> None:
    ctx.run(systemctl(ctx.settings.systemd, "reload", unit), check=True)


@log_status
def restart(ctx: "BaseContext", unit: str) -> None:
    ctx.run(systemctl(ctx.settings.systemd, "restart", unit), check=True)


def is_active(ctx: "BaseContext", unit: str) -> bool:
    r = ctx.run(
        systemctl(ctx.settings.systemd, "--quiet", "--user", "is-active", unit),
        check=False,
    )
    return r.returncode == 0
