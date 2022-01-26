import stat
from pathlib import Path

import pytest

from pglift import util


def test_xdg_config_home(monkeypatch: pytest.MonkeyPatch) -> None:
    with monkeypatch.context() as m:
        m.setenv("XDG_CONFIG_HOME", "/x/y")
        assert util.xdg_config_home() == Path("/x/y")
    with monkeypatch.context() as m:
        try:
            m.delenv("XDG_CONFIG_HOME")
        except KeyError:
            pass
        m.setattr("pathlib.Path.home", lambda: Path("/ho/me"))
        assert util.xdg_config_home() == Path("/ho/me/.config")


def test_xdg_data_home(monkeypatch: pytest.MonkeyPatch) -> None:
    with monkeypatch.context() as m:
        m.setenv("XDG_DATA_HOME", "/x/y")
        assert util.xdg_data_home() == Path("/x/y")
    with monkeypatch.context() as m:
        try:
            m.delenv("XDG_DATA_HOME")
        except KeyError:
            pass
        m.setattr("pathlib.Path.home", lambda: Path("/ho/me"))
        assert util.xdg_data_home() == Path("/ho/me/.local/share")


def test_gen_certificate(tmp_path: Path) -> None:

    util.generate_certificate(tmp_path)
    crt = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    assert crt.exists()
    assert key.exists()
    assert stat.filemode(crt.stat().st_mode) == "-rw-------"
    assert stat.filemode(key.stat().st_mode) == "-rw-------"


def test_total_memory(meminfo: Path) -> None:
    assert util.total_memory(meminfo) == 6166585344.0


def test_total_memory_error(tmp_path: Path) -> None:
    meminfo = tmp_path / "meminfo"
    meminfo.touch()
    with pytest.raises(Exception, match="could not retrieve memory information from"):
        util.total_memory(meminfo)
