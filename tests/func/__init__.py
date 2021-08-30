from contextlib import contextmanager
from typing import Any, Iterator, List, Optional, Union, overload

from typing_extensions import Literal

from pglift import db
from pglift import instance as instance_mod
from pglift.ctx import BaseContext
from pglift.models.system import Instance, InstanceSpec
from pglift.types import Role


def configure_instance(
    ctx: BaseContext,
    i: Union[Instance, InstanceSpec],
    *,
    port: Optional[int] = None,
    **confitems: Any,
) -> None:
    if port is None:
        assert isinstance(i, Instance)
        if port is None:
            port = i.port
    spec = i.as_spec() if isinstance(i, Instance) else i
    instance_mod.configure(ctx, spec, port=port, **confitems)


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
    ctx: BaseContext,
    instance: Instance,
    query: str,
    fetch: Literal[True],
    autocommit: bool = False,
    role: Optional[Role] = None,
    **kwargs: Any,
) -> List[Any]:
    ...


@overload
def execute(
    ctx: BaseContext,
    instance: Instance,
    query: str,
    fetch: bool = False,
    autocommit: bool = False,
    role: Optional[Role] = None,
    **kwargs: Any,
) -> List[Any]:
    ...


def execute(
    ctx: BaseContext,
    instance: Instance,
    query: str,
    fetch: bool = True,
    autocommit: bool = False,
    role: Optional[Role] = None,
    **kwargs: Any,
) -> Optional[List[Any]]:
    if role is None:
        role = ctx.settings.postgresql.surole
    with instance_mod.running(ctx, instance):
        with db.connect(instance, role, autocommit=autocommit, **kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
                if fetch:
                    return cur.fetchall()  # type: ignore[no-any-return]
        return None
