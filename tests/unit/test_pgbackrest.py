from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pglift import exceptions, pgbackrest
from pglift.ctx import Context
from pglift.models.system import Instance
from pglift.settings import PgBackRestSettings, Settings


@pytest.fixture
def pgbackrest_settings(
    need_pgbackrest: None, settings: Settings
) -> PgBackRestSettings:
    assert settings.pgbackrest is not None
    return settings.pgbackrest


def test_make_cmd(
    pg_version: str,
    settings: Settings,
    pgbackrest_settings: PgBackRestSettings,
    instance: Instance,
) -> None:
    assert pgbackrest.make_cmd(instance, pgbackrest_settings, "stanza-upgrade") == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "stanza-upgrade",
    ]


def test_backup_info(
    ctx: Context,
    settings: Settings,
    pgbackrest_settings: PgBackRestSettings,
    pg_version: str,
    instance: Instance,
) -> None:
    with patch.object(ctx, "run") as run:
        run.return_value.stdout = "[]"
        assert (
            pgbackrest.backup_info(ctx, instance, pgbackrest_settings, backup_set="foo")
            == []
        )
    run.assert_called_once_with(
        [
            "/usr/bin/pgbackrest",
            f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
            f"--stanza={pg_version}-test",
            "--set=foo",
            "--output=json",
            "info",
        ],
        check=True,
    )


def test_backup_command(
    pg_version: str,
    settings: Settings,
    pgbackrest_settings: PgBackRestSettings,
    instance: Instance,
) -> None:
    assert pgbackrest.backup_command(
        instance, pgbackrest_settings, type=pgbackrest.BackupType.full
    ) == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "--type=full",
        "--log-level-console=info",
        "--start-fast",
        "backup",
    ]


def test_expire_command(
    pg_version: str,
    settings: Settings,
    pgbackrest_settings: PgBackRestSettings,
    instance: Instance,
) -> None:
    assert pgbackrest.expire_command(instance, pgbackrest_settings) == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "--log-level-console=info",
        "expire",
    ]


def test_restore_command(
    pg_version: str,
    settings: Settings,
    pgbackrest_settings: PgBackRestSettings,
    instance: Instance,
) -> None:
    assert pgbackrest.restore_command(
        instance,
        pgbackrest_settings,
        date=datetime(2003, 1, 1).replace(tzinfo=timezone.utc),
        backup_set="x",
    ) == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "--log-level-console=info",
        "--delta",
        "--link-all",
        "--target-action=promote",
        "--type=time",
        "--target=2003-01-01 00:00:00.000000+0000",
        "--set=x",
        "restore",
    ]


def test_standby_backup(
    ctx: Context, pgbackrest_settings: PgBackRestSettings, standby_instance: Instance
) -> None:
    with pytest.raises(
        exceptions.InstanceStateError,
        match="^backup should be done on primary instance",
    ):
        pgbackrest.backup(ctx, standby_instance, pgbackrest_settings)


def test_standby_restore(
    ctx: Context, pgbackrest_settings: PgBackRestSettings, standby_instance: Instance
) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby",
    ):
        pgbackrest.restore(ctx, standby_instance, pgbackrest_settings)
