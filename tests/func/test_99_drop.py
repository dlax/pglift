import pathlib
import shutil

import pytest
import requests

from pglift import backup, prometheus, systemd


def test_systemd_backup_job(ctx, installed, instance_dropped):
    scheduler = ctx.settings.scheduler
    if scheduler != "systemd":
        pytest.skip(f"not applicable for scheduler method '{scheduler}'")

    instance = instance_dropped
    assert not systemd.is_active(ctx, backup.systemd_timer(instance))
    assert not systemd.is_enabled(ctx, backup.systemd_timer(instance))


@pytest.mark.skipif(
    shutil.which("pgbackrest") is None, reason="pgbackrest is not available"
)
def test_pgbackrest_teardown(ctx, instance_dropped):
    instance = instance_dropped
    pgbackrest_settings = ctx.settings.pgbackrest
    configpath = pathlib.Path(
        str(pgbackrest_settings.configpath).format(instance=instance)
    )
    directory = pathlib.Path(
        str(pgbackrest_settings.directory).format(instance=instance)
    )
    assert not configpath.exists()
    assert not directory.exists()


def test_prometheus_teardown(ctx, instance_dropped):
    instance = instance_dropped
    prometheus_settings = ctx.settings.prometheus
    configpath = pathlib.Path(
        str(prometheus_settings.configpath).format(instance=instance)
    )
    queriespath = pathlib.Path(
        str(prometheus_settings.queriespath).format(instance=instance)
    )
    assert not configpath.exists()
    assert not queriespath.exists()
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_enabled(ctx, prometheus.systemd_unit(instance))
        with pytest.raises(requests.ConnectionError):
            requests.get("http://0.0.0.0:9187/metrics")