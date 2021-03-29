from pathlib import Path

import pytest

from pglib import prometheus


@pytest.fixture
def ctx(ctx):
    ctx.pm.unregister(prometheus)
    return ctx


def test(ctx, installed, instance):
    prometheus_settings = ctx.settings.prometheus
    prometheus.setup(ctx, instance)
    configpath = Path(prometheus_settings.configpath.format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    assert "DATA_SOURCE_URI=localhost:5432" in lines
    queriespath = Path(prometheus_settings.queriespath.format(instance=instance))
    assert queriespath.exists()

    prometheus.revert_setup(ctx, instance)
    assert not configpath.exists()
    assert not queriespath.exists()
