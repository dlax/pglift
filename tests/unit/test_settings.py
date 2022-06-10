import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from pglift import exceptions
from pglift.ctx import Context
from pglift.models.system import Instance
from pglift.settings import (
    DataPath,
    PostgreSQLSettings,
    Settings,
    SiteSettings,
    plugins,
)


def test_json_config_settings_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    settings = tmp_path / "settings.json"
    settings.write_text('{"postgresql": {"root": "/mnt/postgresql"}}')
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{settings}")
        s = SiteSettings()
    assert s.postgresql.root == Path("/mnt/postgresql")
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", '{"postgresql": {"root": "/data/postgres"}}')
        s = SiteSettings()
    assert s.postgresql.root == Path("/data/postgres")
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{tmp_path / 'notfound'}")
        with pytest.raises(FileNotFoundError):
            SiteSettings()


def test_yaml_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    configdir = tmp_path / "pglift"
    configdir.mkdir()
    settings_fpath = configdir / "settings.yaml"
    settings_fpath.write_text("prefix: /tmp")
    with patch(
        "pglift.util.xdg_config", return_value=settings_fpath
    ) as xdg_config, patch("pglift.util.etc_config") as etc_config:
        s = SiteSettings()
    assert str(s.prefix) == "/tmp"
    xdg_config.assert_called_once_with("settings.yaml")
    assert not etc_config.called

    settings_fpath.write_text("hello")
    with patch(
        "pglift.util.xdg_config", return_value=settings_fpath
    ) as xdg_config, patch("pglift.util.etc_config") as etc_config:
        with pytest.raises(exceptions.SettingsError, match="expecting an object"):
            SiteSettings()
    xdg_config.assert_called_once_with("settings.yaml")
    assert not etc_config.called


def test_libpq_environ(ctx: Context, settings: Settings, instance: Instance) -> None:
    assert settings.postgresql.libpq_environ(ctx, instance, base={}) == {
        "PGPASSFILE": str(settings.postgresql.auth.passfile)
    }
    assert settings.postgresql.libpq_environ(
        ctx, instance, base={"PGPASSFILE": "/var/lib/pgsql/pgpass"}
    ) == {"PGPASSFILE": "/var/lib/pgsql/pgpass"}


def test_libpq_environ_password_command(
    ctx: Context, instance: Instance, pg_version: str, tmp_path: Path
) -> None:
    settings = PostgreSQLSettings.parse_obj(
        {
            "auth": {
                "password_command": [
                    sys.executable,
                    "-c",
                    "import sys; print(f'{{sys.argv[1]}}-secret')",
                    "{instance}",
                    "--blah",
                ],
                "passfile": str(tmp_path / "pgpass"),
            }
        }
    )
    assert settings.libpq_environ(ctx, instance, base={}) == {
        "PGPASSFILE": str(tmp_path / "pgpass"),
        "PGPASSWORD": f"{pg_version}/test-secret",
    }


def test_settings(tmp_path: Path) -> None:
    s = Settings(prefix="/")
    assert hasattr(s, "postgresql")
    assert hasattr(s.postgresql, "root")
    assert s.postgresql.root == Path("/srv/pgsql")
    assert s.logpath == Path("/log")

    with pytest.raises(Exception) as e:
        s.postgresql.root = DataPath("/tmp/new_root")
    assert "is immutable and does not support item assignment" in str(e)

    s = Settings.parse_obj(
        {
            "prefix": "/prefix",
            "run_prefix": "/runprefix",
            "postgresql": {"root": str(tmp_path), "pid_directory": "pgsql"},
        }
    )
    assert s.postgresql.root == tmp_path
    assert str(s.postgresql.pid_directory) == "/runprefix/pgsql"


def test_postgresql_versions(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
        s = SiteSettings()
    pgversions = s.postgresql.versions
    assert set(pgversions) == {"10", "11", "12", "13", "14"}
    assert str(pgversions["10"].bindir) == "/opt/pgsql-10/bin"
    assert str(pgversions["12"].bindir) == "/usr/lib/pgsql/12/bin"

    config["postgresql"]["default_version"] = "7"
    config_path.write_text(json.dumps(config))
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{config_path}")
        with pytest.raises(
            ValidationError, match="value is not a valid enumeration member; permitted:"
        ):
            SiteSettings()

    config["postgresql"]["default_version"] = "13"
    config_path.write_text(json.dumps(config))
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", f"@{config_path}")
        s = SiteSettings()
    assert s.postgresql.default_version == "13"


def test_systemd_systemctl() -> None:
    with patch("shutil.which", return_value=None) as which:
        with pytest.raises(ValidationError, match="systemctl command not found"):
            Settings(service_manager="systemd")
    which.assert_called_once_with("systemctl")


def test_systemd_sudo_user() -> None:
    with patch("shutil.which", return_value=True) as which:
        with pytest.raises(
            ValidationError, match="'user' mode cannot be used with 'sudo'"
        ):
            Settings.parse_obj(
                {"service_manager": "systemd", "systemd": {"user": True, "sudo": True}}
            )
    which.assert_called_once_with("systemctl")


def test_plugins() -> None:
    assert set(n for n, _ in plugins(Settings())) == {
        "pgbackrest",
        "prometheus",
        "powa",
        "temboard",
    }
