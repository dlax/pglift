import logging
from pathlib import Path

import pytest

from pglift import ctx
from pglift.settings import Settings


def test_rmtree(
    settings: Settings, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    context = ctx.Context(settings=settings)
    d1 = tmp_path / "d1"
    d1.mkdir()
    d2 = tmp_path / "d2"
    d2.symlink_to(d1, target_is_directory=True)
    with caplog.at_level(logging.WARNING):
        context.rmtree(d2)
    assert (
        f"failed to delete {d2} during tree deletion of {d2}: Cannot call rmtree on a symbolic link"
        in caplog.messages
    )

    caplog.clear()

    with caplog.at_level(logging.WARNING):
        context.rmtree(d1)
    assert not caplog.messages
