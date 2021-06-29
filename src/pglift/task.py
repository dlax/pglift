import collections
import contextlib
import functools
from typing import (
    Any,
    Callable,
    ClassVar,
    Deque,
    Dict,
    Generic,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

from .types import Logger

A = TypeVar("A", bound=Callable[..., Any])

Call = Tuple["task", Tuple[Any, ...], Dict[str, Any]]


class task(Generic[A]):

    _calls: ClassVar[Optional[Deque[Call]]] = None

    def __init__(self, action: A) -> None:
        self.action = action
        self.revert_action: Optional[A] = None
        functools.update_wrapper(self, action)

    def __repr__(self) -> str:
        return f"<task '{self.action.__name__}' at 0x{id(self)}>"

    def _call(self, *args: Any, **kwargs: Any) -> Any:
        if self._calls is not None:
            self._calls.append((self, args, kwargs))
        return self.action(*args, **kwargs)

    __call__ = cast(A, _call)

    def revert(self, revertfn: A) -> A:
        """Decorator to register a 'revert' callback function.

        The revert function must accept the same arguments than its respective
        action.
        """
        self.revert_action = revertfn
        return revertfn


@contextlib.contextmanager
def runner(logger: Logger) -> Iterator[None]:
    """Context manager handling possible revert of a chain to task calls."""
    if task._calls is not None:
        raise RuntimeError("inconsistent task state")
    task._calls = collections.deque()

    try:
        yield None
    except Exception as exc:
        logger.exception(str(exc))
        while True:
            try:
                t, args, kwargs = task._calls.pop()
            except IndexError:
                break
            if t.revert_action:
                t.revert_action(*args, **kwargs)
        raise exc from None
    finally:
        task._calls = None
