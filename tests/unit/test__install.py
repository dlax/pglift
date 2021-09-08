import re

from pglift import _install


def test_postgresql_systemd_unit_template(ctx):
    _install.postgresql_systemd_unit_template(ctx, env="SETTINGS=@settings.json")
    unit = ctx.settings.systemd.unit_path / "postgresql@.service"
    assert unit.exists()
    lines = unit.read_text().splitlines()
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
    assert not unit.exists()


def test_postgres_exporter_systemd_unit_template(ctx):
    _install.postgres_exporter_systemd_unit_template(ctx)
    unit = ctx.settings.systemd.unit_path / "postgres_exporter@.service"
    assert unit.exists()
    lines = unit.read_text().splitlines()
    assert (
        f"EnvironmentFile=-{ctx.settings.prefix}/etc/prometheus/postgres_exporter-%i.conf"
        in lines
    )
    assert (
        "ExecStart=/usr/bin/prometheus-postgres-exporter $POSTGRES_EXPORTER_OPTS"
        in lines
    )
    _install.revert_postgres_exporter_systemd_unit_template(ctx)
    assert not unit.exists()


def test_postgresql_backup_systemd_templates(ctx):
    _install.postgresql_backup_systemd_templates(ctx, env="X-DEBUG=no")
    service_unit = ctx.settings.systemd.unit_path / "postgresql-backup@.service"
    assert service_unit.exists()
    service_lines = service_unit.read_text().splitlines()
    for line in service_lines:
        if line.startswith("ExecStart"):
            execstart = line.split("=", 1)[-1]
            assert re.match(r"^.+/python(3)? -m pglift.backup %i$", execstart)
            break
    else:
        raise AssertionError("ExecStart line not found")
    assert "Environment=X-DEBUG=no" in service_lines
    timer_unit = ctx.settings.systemd.unit_path / "postgresql-backup@.timer"
    assert timer_unit.exists()
    timer_lines = timer_unit.read_text().splitlines()
    assert "OnCalendar=daily" in timer_lines
    _install.revert_postgresql_backup_systemd_templates(ctx)
    assert not timer_unit.exists()
