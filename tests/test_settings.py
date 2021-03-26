from pathlib import Path

import pytest

from pglib.settings import Settings


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


def test_settings_from_env(monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("pglib_postgresql_root", "/tmp/pg")
        s = Settings(postgresql={})
    assert s.postgresql.root == Path("/tmp/pg")
