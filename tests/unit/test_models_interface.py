import io

import yaml

from pglift import util
from pglift.models import interface


class Point(interface.Manifest):
    x: float
    y: float


def test_parse_yaml():
    stream = io.StringIO()
    yaml.dump({"x": 1.2, "y": 3.4}, stream)
    stream.seek(0)
    point = Point.parse_yaml(stream)
    assert point == Point(x=1.2, y=3.4)


def test_yaml():
    point = Point(x=0, y=1.2)
    s = point.yaml()
    assert s == "x: 0.0\ny: 1.2\n"


def test_instance_spec(ctx):
    i = interface.Instance(
        name="test", version="12", prometheus=interface.Instance.Prometheus(port=98)
    ).spec(ctx)
    assert str(i) == "12/test"
    assert i.prometheus.port == 98
    i = interface.Instance(name="test").spec(ctx)
    assert str(i) == f"{util.short_version(ctx.pg_ctl(None).version)}/test"
    assert i.prometheus.port == 9187
