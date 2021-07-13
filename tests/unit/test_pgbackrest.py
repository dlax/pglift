from pglift import pgbackrest


def test_make_cmd(pg_version, settings, instance):
    assert pgbackrest.make_cmd(instance, settings.pgbackrest, "stanza-upgrade") == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "stanza-upgrade",
    ]


def test_backup_command(pg_version, settings, instance):
    assert pgbackrest.backup_command(instance, type=pgbackrest.BackupType.full) == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "--type=full",
        "--repo1-retention-full=9999999",
        "--repo1-retention-archive=9999999",
        "--repo1-retention-diff=9999999",
        "backup",
    ]


def test_expire_command(pg_version, settings, instance):
    assert pgbackrest.expire_command(instance) == [
        "/usr/bin/pgbackrest",
        f"--config={settings.prefix}/etc/pgbackrest/pgbackrest-{pg_version}-test.conf",
        f"--stanza={pg_version}-test",
        "expire",
    ]
