import stat

from pglib import util


def test_gen_certificate(tmp_path):

    util.generate_certificate(tmp_path)
    crt = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    assert crt.exists()
    assert key.exists()
    assert stat.filemode(crt.stat().st_mode) == "-rw-------"
    assert stat.filemode(key.stat().st_mode) == "-rw-------"
