import collections
import functools
from types import TracebackType
from typing import (
    Any,
    Callable,
    ClassVar,
    Deque,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
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


class Runner:
    """Context manager handling possible revert of a chain to task calls."""

    def __init__(self, logger: Logger):
        self.logger = logger

    def __enter__(self) -> None:
        if Task._calls is not None:
            raise RuntimeError("inconsistent task state")
        Task._calls = collections.deque()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if exc_value is not None:
            self.logger.exception(str(exc_value))
            assert Task._calls is not None
            while True:
                try:
                    t, args, kwargs = Task._calls.pop()
                except IndexError:
                    break
                if t.revert_action:
                    t.revert_action(*args, **kwargs)
        Task._calls = None
