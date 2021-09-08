import re

import pytest

from pglift import _install


@pytest.fixture
def fake_systemd_install(monkeypatch):
    install_calls = []
    uninstall_calls = []
    with monkeypatch.context() as m:
        m.setattr(
            "pglift.systemd.install",
            lambda *args, **kwargs: install_calls.append(args),
        )
        m.setattr(
            "pglift.systemd.uninstall",
            lambda *args, **kwargs: uninstall_calls.append(args),
        )
        yield install_calls, uninstall_calls


def test_postgresql_systemd_unit_template(ctx, fake_systemd_install):
    install_calls, uninstall_calls = fake_systemd_install
    _install.postgresql_systemd_unit_template(ctx, env="SETTINGS=@settings.json")
    ((name, content),) = install_calls
    assert name == "postgresql@.service"
    lines = content.splitlines()
    assert "Environment=SETTINGS=@settings.json" in lines
    assert f"PIDFile={ctx.settings.prefix}/run/postgresql/postgresql-%i.pid" in lines
    for line in lines:
        if line.startswith("ExecStart"):
            execstart = line.split("=", 1)[-1]
            assert re.match(r"^.+/python(3)? -m pglift.postgres %i$", execstart)
            break
    else:
        raise AssertionError("ExecStart line not found")
    _install.revert_postgresql_systemd_unit_template(ctx)
    assert uninstall_calls == [("postgresql@.service",)]


def test_postgres_exporter_systemd_unit_template(ctx, fake_systemd_install):
    install_calls, uninstall_calls = fake_systemd_install
    _install.postgres_exporter_systemd_unit_template(ctx)
    ((name, content),) = install_calls
    assert name == "postgres_exporter@.service"
    lines = content.splitlines()
    assert (
        f"EnvironmentFile=-{ctx.settings.prefix}/etc/prometheus/postgres_exporter-%i.conf"
        in lines
    )
    assert (
        "ExecStart=/usr/bin/prometheus-postgres-exporter $POSTGRES_EXPORTER_OPTS"
        in lines
    )
    _install.revert_postgres_exporter_systemd_unit_template(ctx)
    assert uninstall_calls == [("postgres_exporter@.service",)]


def test_postgresql_backup_systemd_templates(ctx, fake_systemd_install):
    install_calls, uninstall_calls = fake_systemd_install
    _install.postgresql_backup_systemd_templates(ctx, env="X-DEBUG=no")
    ((service_name, service_content), (timer_name, timer_content)) = install_calls
    assert service_name == "postgresql-backup@.service"
    service_lines = service_content.splitlines()
    for line in service_lines:
        if line.startswith("ExecStart"):
            execstart = line.split("=", 1)[-1]
            assert re.match(r"^.+/python(3)? -m pglift.backup %i$", execstart)
            break
    else:
        raise AssertionError("ExecStart line not found")
    assert "Environment=X-DEBUG=no" in service_lines
    assert timer_name == "postgresql-backup@.timer"
    timer_lines = timer_content.splitlines()
    assert "OnCalendar=daily" in timer_lines
    _install.revert_postgresql_backup_systemd_templates(ctx)
    assert uninstall_calls == [(service_name,), (timer_name,)]
