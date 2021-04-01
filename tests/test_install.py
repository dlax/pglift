import re

import pytest

from pglib import install
from pglib.settings import PostgreSQLSettings, PrometheusSettings


@pytest.fixture
def fake_systemd_install(monkeypatch):
    install_calls = []
    uninstall_calls = []
    monkeypatch.setattr(
        "pglib.systemd.install",
        lambda *args: install_calls.append(args),
    )
    monkeypatch.setattr(
        "pglib.systemd.uninstall",
        lambda *args: uninstall_calls.append(args),
    )
    return install_calls, uninstall_calls


def test_postgresql_systemd_unit_template(fake_systemd_install, tmp_settings):
    install_calls, uninstall_calls = fake_systemd_install
    settings = PostgreSQLSettings()
    install.postgresql_systemd_unit_template(settings, env="SETTINGS=@settings.json")
    ((name, content),) = install_calls
    assert name == "postgresql@.service"
    lines = content.splitlines()
    assert "Environment=SETTINGS=@settings.json" in lines
    assert "PIDFile=/run/postgresql/postgresql-%i.pid" in lines
    for line in lines:
        if line.startswith("ExecStart"):
            execstart = line.split("=", 1)[-1]
            assert re.match(r"^.+/python(3)? -m pglib.postgres %I$", execstart)
            break
    else:
        raise AssertionError("ExecStart line not found")
    install.revert_postgresql_systemd_unit_template(settings)
    assert uninstall_calls == [("postgresql@.service",)]


def test_postgres_exporter_systemd_unit_template(fake_systemd_install):
    install_calls, uninstall_calls = fake_systemd_install
    settings = PrometheusSettings()
    install.postgres_exporter_systemd_unit_template(settings)
    ((name, content),) = install_calls
    assert name == "postgres_exporter@.service"
    lines = content.splitlines()
    assert "EnvironmentFile=-/etc/prometheus/postgres_exporter-%i.conf" in lines
    assert (
        "ExecStart=/usr/bin/prometheus-postgres-exporter $POSTGRES_EXPORTER_OPTS"
        in lines
    )
    install.revert_postgres_exporter_systemd_unit_template(settings)
    assert uninstall_calls == [("postgres_exporter@.service",)]
