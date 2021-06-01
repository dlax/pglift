import io

import yaml

from pglift import manifest, util


class Point(manifest.Manifest):
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


def test_instance_model(ctx):
    i = manifest.Instance(name="test", version="12").model(ctx)
    assert str(i) == "12/test"
    i = manifest.Instance(name="test").model(ctx)
    assert str(i) == f"{util.short_version(ctx.pg_ctl(None).version)}/test"
