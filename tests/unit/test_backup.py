from pglift import backup


def test_systemd_timer(pg_version, instance):
    assert (
        backup.systemd_timer(instance) == f"postgresql-backup@{pg_version}-test.timer"
    )
