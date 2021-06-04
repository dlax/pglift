from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

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


@contextmanager
def reconfigure_instance(ctx: BaseContext, i: Instance, *, port: int) -> Iterator[None]:
    config = i.config()
    assert config is not None
    initial_port = config.port
    assert initial_port
    configure_instance(ctx, i, port=port)
    try:
        yield
    finally:
        configure_instance(ctx, i, port=initial_port)  # type: ignore[arg-type]
