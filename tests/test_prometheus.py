from pathlib import Path

from pglib import prometheus


def test(ctx, instance, tmp_settings):
    prometheus_settings = tmp_settings.prometheus
    kwargs = {"instance": instance, "settings": prometheus_settings}
    prometheus.setup(ctx, **kwargs)
    configpath = Path(prometheus_settings.configpath.format(instance=instance))
    assert configpath.exists()
    lines = configpath.read_text().splitlines()
    assert "DATA_SOURCE_URI=localhost:5432" in lines
    queriespath = Path(prometheus_settings.queriespath.format(instance=instance))
    assert queriespath.exists()

    prometheus.revert_setup(ctx, **kwargs)
    assert not configpath.exists()
    assert not queriespath.exists()
