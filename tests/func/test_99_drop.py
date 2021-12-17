import pathlib
import shutil

import pytest
import requests
from pgtoolkit.conf import Configuration

from pglift import backup, exceptions, prometheus, systemd
from pglift.ctx import Context
from pglift.models import interface, system


def test_pgpass(
    ctx: Context,
    instance_manifest: interface.Instance,
    instance_dropped: Configuration,
) -> None:
    config = instance_dropped
    passfile = ctx.settings.postgresql.auth.passfile
    surole = interface.instance_surole(ctx.settings, instance_manifest)
    if surole.pgpass and surole.password:
        port = config.port
        pgpass_entries = passfile.read_text().splitlines()
        for line in pgpass_entries:
            assert f"*:{port}:*:{surole.name}:" not in line
    assert passfile.read_text() == "#hostname:port:database:username:password\n"


def test_systemd_backup_job(
    ctx: Context,
    instance: system.Instance,
    instance_dropped: Configuration,
) -> None:
    scheduler = ctx.settings.scheduler
    if scheduler != "systemd":
        pytest.skip(f"not applicable for scheduler method '{scheduler}'")

    assert not systemd.is_active(ctx, backup.systemd_timer(instance))
    assert not systemd.is_enabled(ctx, backup.systemd_timer(instance))


@pytest.mark.skipif(
    shutil.which("pgbackrest") is None, reason="pgbackrest is not available"
)
def test_pgbackrest_teardown(
    ctx: Context, instance: system.Instance, instance_dropped: Configuration
) -> None:
    pgbackrest_settings = ctx.settings.pgbackrest
    configpath = pathlib.Path(
        str(pgbackrest_settings.configpath).format(instance=instance)
    )
    directory = pathlib.Path(
        str(pgbackrest_settings.directory).format(instance=instance)
    )
    assert not configpath.exists()
    assert not directory.exists()


def test_prometheus_teardown(
    ctx: Context, instance: system.Instance, instance_dropped: Configuration
) -> None:
    prometheus_settings = ctx.settings.prometheus
    configpath = pathlib.Path(
        str(prometheus_settings.configpath).format(name=instance.qualname)
    )
    queriespath = pathlib.Path(
        str(prometheus_settings.queriespath).format(name=instance.qualname)
    )
    assert not configpath.exists()
    assert not queriespath.exists()
    if ctx.settings.service_manager == "systemd":
        assert not systemd.is_enabled(ctx, prometheus.systemd_unit(instance.qualname))
        with pytest.raises(requests.ConnectionError):
            requests.get("http://0.0.0.0:9187/metrics")


def test_instance(
    ctx: Context, instance: system.Instance, instance_dropped: Configuration
) -> None:
    with pytest.raises(exceptions.InstanceNotFound, match=str(instance)):
        instance.exists()
