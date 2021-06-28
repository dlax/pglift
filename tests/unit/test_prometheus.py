import pathlib

import pytest

from pglift import prometheus


def test_systemd_unit(pg_version, instance):
    assert (
        prometheus.systemd_unit(instance)
        == f"postgres_exporter@{pg_version}-test.service"
    )


def test_port(ctx, instance):
    port = prometheus.port(ctx, instance)
    assert port == 9817

    configpath = pathlib.Path(
        str(ctx.settings.prometheus.configpath).format(instance=instance)
    )
    configpath.write_text("\nempty\n")
    with pytest.raises(LookupError, match="PG_EXPORTER_WEB_LISTEN_ADDRESS not found"):
        prometheus.port(ctx, instance)

    configpath.write_text("\nPG_EXPORTER_WEB_LISTEN_ADDRESS=42\n")
    with pytest.raises(
        LookupError, match="malformatted PG_EXPORTER_WEB_LISTEN_ADDRESS"
    ):
        prometheus.port(ctx, instance)
