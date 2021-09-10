import json
from pathlib import Path
from unittest.mock import patch

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


def test_yaml_settings(tmp_path, monkeypatch):
    (tmp_path / "settings.yaml").write_text("prefix: /tmp")
    with monkeypatch.context() as m:
        m.setattr("pglift.settings.datapath", tmp_path)
        s = Settings()
    assert str(s.prefix) == "/tmp"

    (tmp_path / "settings.yaml").write_text("hello")
    with monkeypatch.context() as m:
        m.setattr("pglift.settings.datapath", tmp_path)
        with pytest.raises(TypeError, match="expecting an object"):
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


def test_postgresql_versions(monkeypatch, tmp_path):
    config = {
        "postgresql": {
            "bindir": "/usr/lib/pgsql/{version}/bin",
            "versions": {
                "10": {
                    "bindir": "/opt/pgsql-10/bin",
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
    assert set(pgversions) == {"10", "11", "12", "13"}
    assert str(pgversions["10"].bindir) == "/opt/pgsql-10/bin"
    assert str(pgversions["12"].bindir) == "/usr/lib/pgsql/12/bin"

    config["postgresql"]["default_version"] = "7"
    config_path.write_text(json.dumps(config))
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{config_path}")
        with pytest.raises(ValidationError, match="unsupported default version: 7"):
            Settings()

    config["postgresql"]["default_version"] = "13"
    config_path.write_text(json.dumps(config))
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{config_path}")
        s = Settings()
    assert s.postgresql.default_version == "13"


def test_postgresql_surole(monkeypatch, tmp_path):
    # Username and password from init.
    s1 = Settings(postgresql={"surole": {"name": "pgadmin", "password": "blah"}})
    assert s1.postgresql.surole.name == "pgadmin"
    assert s1.postgresql.surole.password is not None
    assert s1.postgresql.surole.password.get_secret_value() == "blah"

    # Username from SETTINGS, password from environment variable.
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", json.dumps({"postgresql": {"surole": {"name": "pga"}}}))
        m.setenv("postgresql_surole_password", "s3kret")
        s2 = Settings()
    assert s2.postgresql.surole.name == "pga"
    assert s2.postgresql.surole.password is not None
    assert s2.postgresql.surole.password.get_secret_value() == "s3kret"

    # Password from SETTINGS.
    with monkeypatch.context() as m:
        m.setenv(
            "SETTINGS",
            json.dumps({"postgresql": {"surole": {"password": "json"}}}),
        )
        s3 = Settings()
    assert s3.postgresql.surole.name == "postgres"
    assert s3.postgresql.surole.password is not None
    assert s3.postgresql.surole.password.get_secret_value() == "json"

    # Username from env and password from secret file, which is NOT supported.
    (tmp_path / "postgresql_surole_password").write_text("file")
    with monkeypatch.context() as m:
        m.setenv("postgresql_surole_name", "adm")
        s4 = Settings(postgresql={"surole": {"_secrets_dir": tmp_path}})
    assert s4.postgresql.surole.name == "adm"
    assert s4.postgresql.surole.password is None


def test_systemd(monkeypatch):
    with patch("shutil.which", return_value=None) as which:
        with pytest.raises(ValidationError, match="systemctl command not found"):
            Settings(service_manager="systemd")
    which.assert_called_once_with("systemctl")
