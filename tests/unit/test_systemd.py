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
    systemd.install("foo", "ahah")
    unit_path = xdg_data_home / "systemd" / "user" / "foo"
    mtime = unit_path.stat().st_mtime
    assert unit_path.read_text() == "ahah"
    systemd.install("foo", "ahah")
    assert unit_path.stat().st_mtime == mtime
    systemd.uninstall("foo")
    assert not unit_path.exists()
    systemd.uninstall("foo")  # no-op
