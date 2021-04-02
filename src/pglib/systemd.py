import functools
import subprocess
from pathlib import Path
from typing import Callable

from .ctx import BaseContext
from .util import xdg_data_home


def template(
    name: str,
    datapath: Path = Path(__file__).parent / "data" / "systemd",
) -> str:
    return (datapath / name).read_text()


def unit_path(name: str) -> Path:
    return xdg_data_home() / "systemd" / "user" / name


def install(name: str, content: str) -> None:
    path = unit_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_text() != content:
        path.write_text(content)


def uninstall(name: str) -> None:
    path = unit_path(name)
    if path.exists():
        path.unlink()


def is_enabled(ctx: BaseContext, unit: str) -> bool:
    r = ctx.run(["systemctl", "--quiet", "--user", "is-enabled", unit], check=False)
    return r.returncode == 0


def enable(ctx: BaseContext, unit: str, *, now: bool = False) -> None:
    cmd = ["systemctl", "--user", "enable", unit]
    if now:
        cmd.append("--now")
    ctx.run(cmd, check=True)


def disable(ctx: BaseContext, unit: str, *, now: bool = True) -> None:
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
