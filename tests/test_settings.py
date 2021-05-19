import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from pglift.settings import DataPath, Settings


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
    s = Settings(prefix="/")
    assert hasattr(s, "postgresql")
    assert hasattr(s.postgresql, "root")
    assert s.postgresql.root == Path("/srv/pgsql")

    with pytest.raises(Exception) as e:
        s.postgresql.root = DataPath("/tmp/new_root")
    assert "is immutable and does not support item assignment" in str(e)

    s = Settings.parse_obj(
        {
            "prefix": "/prefix",
            "postgresql": {"root": str(tmp_path), "pid_directory": "pgsql"},
        }
    )
    assert s.postgresql.root == tmp_path
    assert str(s.postgresql.pid_directory) == "/prefix/run/pgsql"

    pwfile = tmp_path / "surole_password"
    s = Settings.parse_obj({"postgresql": {"initdb": {"auth": ("md5", pwfile)}}})
    assert s.postgresql.initdb.auth
    assert s.postgresql.initdb.auth[1] == pwfile


def test_postgresql_versions(monkeypatch, tmp_path):
    config = {
        "postgresql": {
            "bindir": "/usr/lib/pgsql/{version}/bin",
            "versions": {
                "9.6": {
                    "bindir": "/opt/pgsql-9.6/bin",
                },
            },
        },
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{config_path}")
        s = Settings()
    pgversions = s.postgresql.versions
    assert set(pgversions) == {"9.6", "10", "11", "12", "13"}
    assert str(pgversions["9.6"].bindir) == "/opt/pgsql-9.6/bin"
    assert str(pgversions["12"].bindir) == "/usr/lib/pgsql/12/bin"

    config["postgresql"]["default_version"] = "7"
    config_path.write_text(json.dumps(config))
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{config_path}")
        with pytest.raises(ValidationError, match="unsupported default version: 7"):
            Settings()
