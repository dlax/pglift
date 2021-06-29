import re

import pytest

from pglift import task


def test_task():
    @task.task
    def neg(x: int) -> int:
        return -x

    assert re.match(r"<task 'neg' at 0x(\d+)>" "", repr(neg))

    assert neg(1) == -1
    assert neg.revert_action is None

    @neg.revert
    def revert_neg(x: int) -> int:
        return -x

    assert neg.revert_action
    assert neg.revert_action(-1) == 1


def test_runner_state(logger):
    with pytest.raises(RuntimeError, match="inconsistent task state"):
        with task.runner(logger):
            with task.runner(logger):
                pass

    with pytest.raises(ValueError, match="expected"):
        with task.runner(logger):
            assert task.task._calls is not None
            raise ValueError("expected")
    assert task.task._calls is None


def test_runner(logger):
    values = set()

    @task.task
    def add(x: int, fail: bool = False) -> None:
        values.add(x)
        if fail:
            raise RuntimeError("oups")

    add(1)
    assert values == {1}

    with pytest.raises(RuntimeError, match="oups"):
        with task.runner(logger):
            add(2, fail=True)
    # no revert action
    assert values == {1, 2}

    @add.revert
    def remove(x: int, fail: bool = False) -> None:
        try:
            values.remove(x)
        except KeyError:
            pass

    with pytest.raises(RuntimeError, match="oups"):
        with task.runner(logger):
            add(3, fail=False)
            add(4, fail=True)
    assert values == {1, 2}

    @add.revert
    def remove_fail(x: int, fail: bool = False) -> None:
        if fail:
            raise ValueError("failed to fail")

    with pytest.raises(ValueError, match="failed to fail"):
        with task.runner(logger):
            add(3, fail=False)
            add(4, fail=True)
    assert values == {1, 2, 3, 4}
