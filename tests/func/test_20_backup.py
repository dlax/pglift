import pytest

from pglift import backup
from pglift import instance as instance_mod
from pglift import systemd


def test_systemd_backup_job(ctx, installed, instance):
    scheduler = ctx.settings.scheduler
    if scheduler != "systemd":
        pytest.skip(f"not applicable for scheduler method '{scheduler}'")

    assert systemd.is_enabled(ctx, backup.systemd_timer(instance))

    assert not systemd.is_active(ctx, backup.systemd_timer(instance))
    with instance_mod.running(ctx, instance, run_hooks=True):
        assert systemd.is_active(ctx, backup.systemd_timer(instance))
    assert not systemd.is_active(ctx, backup.systemd_timer(instance))