import logging

import pytest

from pglift import systemd


@pytest.fixture
def xdg_data_home(monkeypatch, tmp_path):
    with monkeypatch.context() as m:
        m.setattr("pglift.systemd.xdg_data_home", lambda: tmp_path)
        yield tmp_path


def test_unit_path(xdg_data_home):
    assert systemd.unit_path("foo") == xdg_data_home / "systemd" / "user" / "foo"


def test_install_uninstall(xdg_data_home):
    logger = logging.getLogger(__name__)
    systemd.install("foo", "ahah", logger=logger)
    unit_path = xdg_data_home / "systemd" / "user" / "foo"
    mtime = unit_path.stat().st_mtime
    assert unit_path.read_text() == "ahah"
    systemd.install("foo", "ahah", logger=logger)
    assert unit_path.stat().st_mtime == mtime
    with pytest.raises(FileExistsError, match="not overwriting"):
        systemd.install("foo", "ahahah", logger=logger)
    systemd.uninstall("foo", logger=logger)
    assert not unit_path.exists()
    systemd.uninstall("foo", logger=logger)  # no-op
