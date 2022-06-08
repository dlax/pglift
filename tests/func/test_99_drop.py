import pathlib

import pytest
import requests
from pgtoolkit.conf import Configuration

from pglift import backup, exceptions, systemd
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.prometheus import impl as prometheus
from pglift.settings import PgBackRestSettings, PrometheusSettings

from . import AuthType


def test_pgpass(
    ctx: Context,
    postgresql_auth: AuthType,
    instance_manifest: interface.Instance,
    standby_instance_dropped: Configuration,
    instance_dropped: Configuration,
) -> None:
    if postgresql_auth != AuthType.pgpass:
        pytest.skip("not applicable")
    passfile = ctx.settings.postgresql.auth.passfile
    assert not passfile.exists()


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


def test_pgbackrest_teardown(
    ctx: Context,
    pgbackrest_settings: PgBackRestSettings,
    instance: system.Instance,
    instance_dropped: Configuration,
) -> None:
    configpath = pathlib.Path(
        str(pgbackrest_settings.configpath).format(instance=instance)
    )
    directory = pathlib.Path(
        str(pgbackrest_settings.directory).format(instance=instance)
    )
    lockpath = pathlib.Path(str(pgbackrest_settings.lockpath).format(instance=instance))
    spoolpath = pathlib.Path(
        str(pgbackrest_settings.spoolpath).format(instance=instance)
    )
    assert not configpath.exists()
    assert not directory.exists()
    assert not lockpath.exists()
    assert not spoolpath.exists()


def test_prometheus_teardown(
    ctx: Context,
    prometheus_settings: PrometheusSettings,
    instance: system.Instance,
    instance_dropped: Configuration,
) -> None:
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
