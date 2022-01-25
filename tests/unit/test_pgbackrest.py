from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pglift import exceptions, pgbackrest
from pglift.ctx import Context
from pglift.models.system import Instance
from pglift.settings import Settings


def test_make_cmd(pg_version: str, settings: Settings, instance: Instance) -> None:
    assert pgbackrest.make_cmd(instance, settings.pgbackrest, "stanza-upgrade") == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "stanza-upgrade",
    ]


def test_backup_info(
    ctx: Context, settings: Settings, pg_version: str, instance: Instance
) -> None:
    with patch.object(ctx, "run") as run:
        run.return_value.stdout = "[]"
        assert pgbackrest.backup_info(ctx, instance, backup_set="foo") == []
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
    pg_version: str, settings: Settings, instance: Instance
) -> None:
    assert pgbackrest.backup_command(instance, type=pgbackrest.BackupType.full) == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "--type=full",
        "--log-level-console=info",
        "--start-fast",
        "backup",
    ]


def test_expire_command(
    pg_version: str, settings: Settings, instance: Instance
) -> None:
    assert pgbackrest.expire_command(instance) == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "--log-level-console=info",
        "expire",
    ]


def test_restore_command(
    pg_version: str, settings: Settings, instance: Instance
) -> None:
    assert pgbackrest.restore_command(
        instance, date=datetime(2003, 1, 1).replace(tzinfo=timezone.utc), backup_set="x"
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


def test_standby_backup(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceStateError,
        match="^backup should be done on primary instance",
    ):
        pgbackrest.backup(ctx, standby_instance)


def test_standby_restore(ctx: Context, standby_instance: Instance) -> None:
    with pytest.raises(
        exceptions.InstanceReadOnlyError,
        match=f"^{standby_instance.version}/standby is a read-only standby",
    ):
        pgbackrest.restore(ctx, standby_instance)
