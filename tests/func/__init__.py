from contextlib import contextmanager
from functools import partial
from typing import Any, Iterator, List, Optional, overload

from typing_extensions import Literal

from pglift import db
from pglift import instance as instance_mod
from pglift.ctx import BaseContext
from pglift.models import interface
from pglift.models.system import Instance
from pglift.types import Role


def configure_instance(
    ctx: BaseContext,
    manifest: interface.Instance,
    *,
    port: Optional[int] = None,
    **confitems: Any,
) -> None:
    if port is None:
        port = manifest.port
    values = dict(confitems, port=port)
    instance_mod.configure(ctx, manifest, values=values)


@contextmanager
def reconfigure_instance(
    ctx: BaseContext, i: Instance, manifest: interface.Instance, *, port: int
) -> Iterator[None]:
    config = i.config()
    assert config is not None
    initial_port = config.port
    assert initial_port
    configure_instance(ctx, manifest, port=port)
    try:
        yield
    finally:
        configure_instance(ctx, manifest, port=initial_port)  # type: ignore[arg-type]


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
        connect = partial(db.superuser_connect, ctx)
    elif role.password:
        connect = partial(
            db.connect, user=role.name, password=role.password.get_secret_value()
        )
    else:
        connect = partial(db.connect, user=role.name)
    with instance_mod.running(ctx, instance):
        with connect(instance, autocommit=autocommit, **kwargs) as conn:
            cur = conn.execute(query)
            conn.commit()
            if fetch:
                return cur.fetchall()
        return None
