import pathlib

import pytest

from pglift import exceptions, prometheus
from pglift.models import interface


def test_systemd_unit(pg_version, instance):
    assert (
        prometheus.systemd_unit(instance.stanza)
        == f"postgres_exporter@{pg_version}-test.service"
    )


def test_port(ctx, instance):
    port = prometheus.port(ctx, instance.stanza)
    assert port == 9817

    configpath = pathlib.Path(
        str(ctx.settings.prometheus.configpath).format(stanza=instance.stanza)
    )
    configpath.write_text("\nempty\n")
    with pytest.raises(LookupError, match="PG_EXPORTER_WEB_LISTEN_ADDRESS not found"):
        prometheus.port(ctx, instance.stanza)

    configpath.write_text("\nPG_EXPORTER_WEB_LISTEN_ADDRESS=42\n")
    with pytest.raises(
        LookupError, match="malformatted PG_EXPORTER_WEB_LISTEN_ADDRESS"
    ):
        prometheus.port(ctx, instance.stanza)


def test_apply(ctx, instance):
    m = interface.PostgresExporter(name=instance.stanza, dsn="", port=123)
    with pytest.raises(exceptions.InstanceStateError, match="exists locally"):
        prometheus.apply(ctx, m)
