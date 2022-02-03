import logging
import re
from typing import List

import pytest

from pglift import task


class SimpleDisplayer:
    def __init__(self) -> None:
        self.records: List[str] = []

    def clear(self) -> None:
        self.records.clear()

    def handle(self, msg: str) -> None:
        self.records.append(msg)


def test_task() -> None:
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


def test_transaction_state() -> None:
    with pytest.raises(RuntimeError, match="inconsistent task state"):
        with task.transaction():
            with task.transaction():
                pass

    with pytest.raises(ValueError, match="expected"):
        with task.transaction():
            assert task.Task._calls is not None
            raise ValueError("expected")
    assert task.Task._calls is None


def test_transaction(caplog: pytest.LogCaptureFixture) -> None:
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
        with task.displayer_installed(displayer), task.transaction():
            add(2, fail=True)
    # no revert action
    assert values == {1, 2}
    assert displayer.records == ["add 2 to values"]

    displayer.clear()

    @add.revert("remove {x} from values (fail={fail})")
    def remove(x: int, fail: bool = False) -> None:
        try:
            values.remove(x)
        except KeyError:
            pass

    with pytest.raises(RuntimeError, match="oups"):
        with task.displayer_installed(displayer), task.transaction():
            add(3, fail=False)
            add(4, fail=True)
    assert values == {1, 2}
    assert displayer.records == [
        "add 3 to values",
        "add 4 to values",
        "remove 4 from values (fail=True)",
        "remove 3 from values (fail=False)",
    ]

    displayer.clear()

    @add.revert("remove numbers, failed")
    def remove_fail(x: int, fail: bool = False) -> None:
        if fail:
            raise ValueError("failed to fail")

    with pytest.raises(ValueError, match="failed to fail"):
        with task.transaction():
            add(3, fail=False)
            add(4, fail=True)
    assert values == {1, 2, 3, 4}

    @task.task("... INTR")
    def intr() -> None:
        raise KeyboardInterrupt

    caplog.clear()
    with pytest.raises(KeyboardInterrupt), caplog.at_level(logging.WARNING):
        with task.transaction():
            intr()
    assert caplog.messages == [f"{intr} interrupted"]
