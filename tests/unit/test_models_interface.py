import io

import pydantic
import pytest
import yaml

from pglift.models import interface


class Point(interface.Manifest):
    x: float
    y: float


def test_parse_yaml() -> None:
    stream = io.StringIO()
    yaml.dump({"x": 1.2, "y": 3.4}, stream)
    stream.seek(0)
    point = Point.parse_yaml(stream)
    assert point == Point(x=1.2, y=3.4)


def test_yaml() -> None:
    point = Point(x=0, y=1.2)
    s = point.yaml()
    assert s == "x: 0.0\ny: 1.2\n"


def test_postgresexporter() -> None:
    m = interface.PostgresExporter(name="12-x", dsn="dbname=postgres", port=9876)
    assert m.dsn == "dbname=postgres"
    with pytest.raises(pydantic.ValidationError):
        interface.PostgresExporter(dsn="x=y", port=9876)
