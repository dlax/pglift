import collections
import contextlib
import functools
import inspect
from types import TracebackType
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
    Type,
    TypeVar,
    cast,
)

from typing_extensions import Protocol

from ._compat import nullcontext
from .types import Logger

A = TypeVar("A", bound=Callable[..., Any])

Call = Tuple["Task", Tuple[Any, ...], Dict[str, Any]]


class Displayer(Protocol):
    def __enter__(self) -> Any:
        ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        ...

    @contextlib.contextmanager
    def handle(self, msg: str) -> Iterator[None]:
        ...


class Task(Generic[A]):

    _calls: ClassVar[Optional[Deque[Call]]] = None
    displayer: ClassVar[Optional[Displayer]] = None

    def __init__(self, title: str, action: A) -> None:
        assert title, "expecting a non-empty title"
        self.title = title[0].upper() + title[1:]
        self.action = action
        self.revert_action: Optional[A] = None
        functools.update_wrapper(self, action)

    def __repr__(self) -> str:
        return f"<Task '{self.action.__name__}' at 0x{id(self)}>"

    def _call(self, *args: Any, **kwargs: Any) -> Any:
        if self._calls is not None:
            self._calls.append((self, args, kwargs))
        s = inspect.signature(self.action)
        b = s.bind(*args, **kwargs)
        b.apply_defaults()
        with self.display(self.title.format(**b.arguments)):
            return self.action(*args, **kwargs)

    __call__ = cast(A, _call)

    @contextlib.contextmanager
    def display(self, title: str) -> Iterator[None]:
        if self.displayer is None:
            cm = nullcontext()
        else:
            cm = self.displayer.handle(title)

        with cm:
            yield None

    def revert(self, title: str) -> Callable[[A], A]:
        """Decorator to register a 'revert' callback function.

        The revert function must accept the same arguments than its respective
        action.
        """
        title = title[0].upper() + title[1:]

        def decorator(revertfn: A) -> A:
            s = inspect.signature(revertfn)

            @functools.wraps(revertfn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                b = s.bind(*args, **kwargs)
                b.apply_defaults()
                with self.display(title.format(**b.arguments)):
                    return revertfn(*args, **kwargs)

            w = cast(A, wrapper)
            self.revert_action = w
            return w

        return decorator


def task(title: str) -> Callable[[A], Task[A]]:
    def mktask(fn: A) -> Task[A]:
        return functools.wraps(fn)(Task(title, fn))

    return mktask


class Runner:
    """Context manager handling possible revert of a chain to task calls."""

    def __init__(self, logger: Logger, displayer: Optional[Displayer] = None):
        self.logger = logger
        self.displayer = displayer

    def __enter__(self) -> None:
        if Task._calls is not None:
            raise RuntimeError("inconsistent task state")
        Task._calls = collections.deque()

        Task.displayer = self.displayer
        if self.displayer is not None:
            self.displayer.__enter__()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        try:
            if exc_value is not None:
                if exc_type is KeyboardInterrupt:
                    if Task._calls:
                        self.logger.warning("%s interrupted", Task._calls[-1][0])
                else:
                    self.logger.exception(str(exc_value))
                assert Task._calls is not None
                while True:
                    try:
                        t, args, kwargs = Task._calls.pop()
                    except IndexError:
                        break
                    if t.revert_action:
                        t.revert_action(*args, **kwargs)
        finally:
            Task._calls = None
            Task.displayer = None

            if self.displayer is not None:
                self.displayer.__exit__(exc_type, exc_value, traceback)
