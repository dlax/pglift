from pglift import backup
from pglift.models.system import Instance


def test_systemd_timer(pg_version: str, instance: Instance) -> None:
    assert backup.systemd_timer(instance) == f"pglift-backup@{pg_version}-test.timer"
