from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional, Tuple, Union, overload

from typing_extensions import Literal

from pglift import db
from pglift import instance as instance_mod
from pglift.ctx import BaseContext
from pglift.models.system import Instance, InstanceSpec


def configure_instance(
    ctx: BaseContext,
    i: Union[Instance, InstanceSpec],
    *,
    port: Optional[int] = None,
    socket_path: Optional[Path] = None,
    **confitems: Any
) -> None:
    if port is None or socket_path is None:
        assert isinstance(i, Instance)
        if port is None:
            port = i.port
        if not socket_path:
            config = i.config()
            socket_path = Path(config.unix_socket_directories)  # type: ignore[arg-type]
    spec = i.as_spec() if isinstance(i, Instance) else i
    instance_mod.configure(
        ctx, spec, port=port, unix_socket_directories=str(socket_path), **confitems
    )


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


@overload
def execute(
    ctx: BaseContext, instance: Instance, query: str, fetch: Literal[False]
) -> None:
    ...


@overload
def execute(
    ctx: BaseContext, instance: Instance, query: str, fetch: Literal[True]
) -> Tuple[Any, ...]:
    ...


@overload
def execute(ctx: BaseContext, instance: Instance, query: str) -> Tuple[Any, ...]:
    ...


def execute(
    ctx: BaseContext, instance: Instance, query: str, fetch: bool = True
) -> Optional[Tuple[Any, ...]]:
    with instance_mod.running(ctx, instance):
        with db.connect(instance, ctx.settings.postgresql.surole) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
                if fetch:
                    return cur.fetchall()  # type: ignore[no-any-return]
        return None
