from pathlib import Path

import pytest

from pglift import conf


@pytest.fixture(params=["relative", "absolute"])
def log_directory(instance, request, tmp_path):
    if request.param == "relative":
        path = Path("loghere")
        return path, instance.datadir / path
    elif request.param == "absolute":
        path = tmp_path / "log" / "here"
        return path, path


def test_log_directory(instance, log_directory):
    log_dir, abs_log_dir = log_directory
    assert not abs_log_dir.exists()
    conf.create_log_directory(instance, log_dir)
    assert abs_log_dir.exists()
    conf.remove_log_directory(instance, log_dir)
    assert not abs_log_dir.exists()
    assert abs_log_dir.parent.exists()
