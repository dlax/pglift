import contextlib
import logging
import re
from typing import Any, Iterator, List, Tuple

import pytest

from pglift import task


class SimpleDisplayer:
    def __enter__(self) -> "SimpleDisplayer":
        self.records: List[Tuple[str, bool]] = []
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    @contextlib.contextmanager
    def handle(self, msg: str) -> Iterator[None]:
        try:
            yield None
        except Exception:
            self.records.append((msg, False))
            raise
        else:
            self.records.append((msg, True))


def test_task():
    @task.task("negate")
    def neg(x: int) -> int:
        return -x

    assert re.match(r"<Task 'neg' at 0x(\d+)>" "", repr(neg))

    assert neg(1) == -1
    assert neg.revert_action is None

    @neg.revert("negate again")
    def revert_neg(x: int) -> int:
        return -x

    assert neg.revert_action
    assert neg.revert_action(-1) == 1


def test_runner_state(logger):
    with pytest.raises(RuntimeError, match="inconsistent task state"):
        with task.Runner(logger):
            with task.Runner(logger):
                pass

    with pytest.raises(ValueError, match="expected"):
        with task.Runner(logger):
            assert task.Task._calls is not None
            raise ValueError("expected")
    assert task.Task._calls is None


def test_runner(logger, caplog):
    values = set()

    @task.task("add {x} to values")
    def add(x: int, fail: bool = False) -> None:
        values.add(x)
        if fail:
            raise RuntimeError("oups")

    add(1)
    assert values == {1}

    displayer = SimpleDisplayer()
    with pytest.raises(RuntimeError, match="oups"):
        with task.Runner(logger, displayer):
            add(2, fail=True)
    # no revert action
    assert values == {1, 2}
    assert displayer.records == [("Add 2 to values", False)]

    @add.revert("remove {x} from values (fail={fail})")
    def remove(x: int, fail: bool = False) -> None:
        try:
            values.remove(x)
        except KeyError:
            pass

    with pytest.raises(RuntimeError, match="oups"):
        with task.Runner(logger, displayer):
            add(3, fail=False)
            add(4, fail=True)
    assert values == {1, 2}
    assert displayer.records == [
        ("Add 3 to values", True),
        ("Add 4 to values", False),
        ("Remove 4 from values (fail=True)", True),
        ("Remove 3 from values (fail=False)", True),
    ]

    @add.revert("remove numbers, failed")
    def remove_fail(x: int, fail: bool = False) -> None:
        if fail:
            raise ValueError("failed to fail")

    with pytest.raises(ValueError, match="failed to fail"):
        with task.Runner(logger):
            add(3, fail=False)
            add(4, fail=True)
    assert values == {1, 2, 3, 4}

    @task.task("... INTR")
    def intr() -> None:
        raise KeyboardInterrupt

    caplog.clear()
    with pytest.raises(KeyboardInterrupt), caplog.at_level(logging.WARNING):
        with task.Runner(logger):
            intr()
    assert caplog.messages == [f"{intr} interrupted"]
