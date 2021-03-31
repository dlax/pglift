import re

from pglib import install
from pglib.settings import PostgreSQLSettings


def test_postgresql_systemd_unit_template(monkeypatch):
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
