from pathlib import Path
from typing import Optional

from pglift import instance
from pglift.ctx import BaseContext
from pglift.model import Instance


def configure_instance(
    ctx: BaseContext,
    i: Instance,
    *,
    port: Optional[int] = None,
    socket_path: Optional[Path] = None
) -> None:
    if port is None or socket_path is None:
        config = i.config()
        assert config is not None
        if port is None:
            port = config.port  # type: ignore[assignment]
        if not socket_path:
            socket_path = Path(config.unix_socket_directories)  # type: ignore[arg-type]
    instance.configure(ctx, i, port=port, unix_socket_directories=str(socket_path))
