import pytest

from pglib import instance as instance_mod
from pglib import settings
from pglib.ctx import Context
from pglib.model import Instance


@pytest.fixture
def tmp_settings(tmp_path):

    pgbackrest_root = tmp_path / "pgbackrest"
    pgbackrest_root.mkdir()

    prometheus_root = tmp_path / "prometheus"
    prometheus_root.mkdir()

    return settings.to_config(
        {
            "PGLIB_POSTGRESQL_ROOT": str(tmp_path),
            "PGLIB_PGBACKREST_CONFIGPATH": str(
                pgbackrest_root / "{instance.version}" / "pgbackrest.conf"
            ),
            "PGLIB_PGBACKREST_DIRECTORY": str(
                tmp_path / "{instance.version}" / "backups"
            ),
            "PGLIB_PGBACKREST_LOGPATH": str(
                pgbackrest_root / "{instance.version}" / "logs"
            ),
            "PGLIB_PROMETHEUS_CONFIGPATH": str(
                prometheus_root / "{instance.version}" / "postgres_exporter.conf"
            ),
            "PGLIB_PROMETHEUS_QUERIESPATH": str(
                prometheus_root / "{instance.version}" / "queries.yaml"
            ),
        },
    )


@pytest.fixture
def ctx(tmp_settings):
    return Context(settings=tmp_settings)


@pytest.fixture
def instance(ctx, tmp_path):
    i = Instance.default_version("test", ctx=ctx)
    instance_mod.init(ctx, i)
    instance_mod.configure(ctx, i, unix_socket_directories=str(tmp_path))
    return i
