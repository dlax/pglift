import functools
from typing import Any, Callable, Generic, Optional, TypeVar, cast

A = TypeVar("A", bound=Callable[..., Any])


class task(Generic[A]):
    def __init__(self, action: A) -> None:
        self.action = action
        self.revert_action: Optional[A] = None
        functools.update_wrapper(self, action)

    def __repr__(self) -> str:
        return f"<task '{self.action.__name__}' at 0x{id(self)}>"

    def _call(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return self.action(*args, **kwargs)
        except Exception as exc:
            if self.revert_action:
                self.revert_action(*args, **kwargs)
            raise exc from None

    __call__ = cast(A, _call)

    def revert(self, revertfn: A) -> A:
        """Decorator to register a 'revert' callback function.

        The revert function must accept the same arguments than its respective
        action.
        """
        self.revert_action = revertfn
        return revertfn
