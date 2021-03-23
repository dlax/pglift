import pytest

from pglib import settings
from pglib.ctx import Context


@pytest.fixture
def ctx():
    return Context()


@pytest.fixture
def tmp_settings(tmp_path):

    pgbackrest_root = tmp_path / "pgbackrest"
    pgbackrest_root.mkdir()

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
        },
    )
