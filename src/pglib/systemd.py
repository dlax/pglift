from pathlib import Path

from .ctx import BaseContext
from .util import xdg_data_home


def template(
    name: str,
    datapath: Path = Path(__file__).parent / "data" / "systemd",
) -> str:
    return (datapath / f"{name}.service").read_text()


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


def start(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "start", unit], check=True)


def stop(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "stop", unit], check=True)


def reload(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "reload", unit], check=True)


def restart(ctx: BaseContext, unit: str) -> None:
    ctx.run(["systemctl", "--user", "restart", unit], check=True)


def is_active(ctx: BaseContext, unit: str) -> bool:
    r = ctx.run(["systemctl", "--quiet", "--user", "is-active", unit], check=False)
    return r.returncode == 0
