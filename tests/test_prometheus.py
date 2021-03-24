from pathlib import Path

from pglib import prometheus


def test(ctx, instance):
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
