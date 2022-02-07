import re

from pglift import _install
from pglift.ctx import Context


def test_postgresql_systemd_unit_template(ctx: Context) -> None:
    _install.postgresql_systemd_unit_template(
        ctx, env="SETTINGS=@settings.json", header="# Postgres managed by pglift"
    )
    unit = ctx.settings.systemd.unit_path / "pglift-postgresql@.service"
    assert unit.exists()
    lines = unit.read_text().splitlines()
    assert lines[0] == "# Postgres managed by pglift"
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


def test_postgresql_backup_systemd_templates(ctx: Context) -> None:
    _install.postgresql_backup_systemd_templates(ctx, env="X-DEBUG=no")
    service_unit = ctx.settings.systemd.unit_path / "pglift-backup@.service"
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
    timer_unit = ctx.settings.systemd.unit_path / "pglift-backup@.timer"
    assert timer_unit.exists()
    timer_lines = timer_unit.read_text().splitlines()
    assert "OnCalendar=daily" in timer_lines
    _install.revert_postgresql_backup_systemd_templates(ctx)
    assert not timer_unit.exists()
