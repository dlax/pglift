import pydantic
import pytest

from pglift.models import interface


def test_postgresexporter() -> None:
    m = interface.PostgresExporter(name="12-x", dsn="dbname=postgres", port=9876)
    assert m.dsn == "dbname=postgres"
    with pytest.raises(pydantic.ValidationError):
        interface.PostgresExporter(dsn="x=y", port=9876)
