import re

import pytest

from pglib import task


def test_task():
    values = set()

    @task.task
    def add(x: int, fail: bool = False) -> None:
        values.add(x)
        if fail:
            raise RuntimeError("oups")

    assert re.match(r"<task 'add' at 0x(\d+)>" "", repr(add))

    add(1)
    assert values == {1}

    with pytest.raises(RuntimeError):
        add(2, fail=True)
    # no revert action
    assert values == {1, 2}

    @add.revert
    def remove(x: int, fail: bool = False) -> None:
        try:
            values.remove(x)
        except KeyError:
            pass

    with pytest.raises(RuntimeError):
        add(3, fail=True)
    assert values == {1, 2}
