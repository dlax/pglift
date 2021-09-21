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

Call = Tuple["Task", Tuple[Any, ...], Dict[str, Any]]


class Task(Generic[A]):

    _calls: ClassVar[Optional[Deque[Call]]] = None

    def __init__(self, title: str, action: A) -> None:
        self.title = title
        self.action = action
        self.revert_action: Optional[A] = None
        functools.update_wrapper(self, action)

    def __repr__(self) -> str:
        return f"<Task '{self.action.__name__}' at 0x{id(self)}>"

    def _call(self, *args: Any, **kwargs: Any) -> Any:
        if self._calls is not None:
            self._calls.append((self, args, kwargs))
        return self.action(*args, **kwargs)

    __call__ = cast(A, _call)

    def revert(self, title: str) -> Callable[[A], A]:
        """Decorator to register a 'revert' callback function.

        The revert function must accept the same arguments than its respective
        action.
        """

        def decorator(revertfn: A) -> A:
            self.revert_action = revertfn
            return revertfn

        return decorator


def task(title: str) -> Callable[[A], Task[A]]:
    def mktask(fn: A) -> Task[A]:
        return functools.wraps(fn)(Task(title, fn))

    return mktask


@contextlib.contextmanager
def runner(logger: Logger) -> Iterator[None]:
    """Context manager handling possible revert of a chain to task calls."""
    if Task._calls is not None:
        raise RuntimeError("inconsistent task state")
    Task._calls = collections.deque()

    try:
        yield None
    except Exception as exc:
        logger.exception(str(exc))
        while True:
            try:
                t, args, kwargs = Task._calls.pop()
            except IndexError:
                break
            if t.revert_action:
                t.revert_action(*args, **kwargs)
        raise exc from None
    finally:
        Task._calls = None
