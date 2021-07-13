from pglift import prometheus


def test_systemd_unit(pg_version, instance):
    assert (
        prometheus.systemd_unit(instance)
        == f"postgres_exporter@{pg_version}-test.service"
    )
