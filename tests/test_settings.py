from pathlib import Path

import pytest

from pglib.settings import Settings


def test_json_config_settings_source(monkeypatch, tmp_path):
    settings = tmp_path / "settings.json"
    settings.write_text('{"postgresql": {"root": "/mnt/postgresql"}}')
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{settings}")
        s = Settings()
    assert s.postgresql.root == Path("/mnt/postgresql")
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", '{"postgresql": {"root": "/data/postgres"}}')
        s = Settings()
    assert s.postgresql.root == Path("/data/postgres")
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{tmp_path / 'notfound'}")
        with pytest.raises(FileNotFoundError):
            Settings()


def test_settings(tmp_path):
    s = Settings()
    assert hasattr(s, "postgresql")
    assert hasattr(s.postgresql, "root")
    assert s.postgresql.root == Path("/var/lib/pgsql")

    with pytest.raises(Exception) as e:
        s.postgresql.root = Path("/tmp/new_root")
    assert "is immutable and does not support item assignment" in str(e)

    s = Settings.parse_obj({"postgresql": {"root": str(tmp_path)}})
    assert s.postgresql.root == tmp_path

    pwfile = tmp_path / "surole_password"
    s = Settings.parse_obj({"postgresql": {"initdb_auth": ("md5", pwfile)}})
    assert s.postgresql.initdb_auth
    assert s.postgresql.initdb_auth[1] == pwfile


def test_settings_from_env(monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("pglib_postgresql_root", "/tmp/pg")
        s = Settings(postgresql={})
    assert s.postgresql.root == Path("/tmp/pg")
