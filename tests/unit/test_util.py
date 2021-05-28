import stat
from pathlib import Path

from pglift import util


def test_xdg_data_home(monkeypatch):
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


def test_gen_certificate(tmp_path):

    util.generate_certificate(tmp_path)
    crt = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    assert crt.exists()
    assert key.exists()
    assert stat.filemode(crt.stat().st_mode) == "-rw-------"
    assert stat.filemode(key.stat().st_mode) == "-rw-------"