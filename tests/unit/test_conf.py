from pathlib import Path

import pytest

from pglift import conf


def test_read(pg_version, settings, tmp_path):
    datadir = tmp_path
    (datadir / "postgresql.auto.conf").touch()
    postgresql_conf = datadir / "postgresql.conf"
    postgresql_conf.touch()
    postgresql_conf.write_text("\n".join(["bonjour = hello", "port=1234"]))

    config = conf.read(tmp_path)
    config.bonjour == "hello"
    config.port == 1234

    user_conf = datadir / "conf.pglift.d" / "user.conf"
    with pytest.raises(FileNotFoundError, match=str(user_conf)):
        conf.read(datadir, True)
    user_conf.parent.mkdir(parents=True)
    user_conf.write_text("\n".join(["port=5555"]))
    mconf = conf.read(datadir, True)
    assert mconf is not None
    assert mconf.port == 5555


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
