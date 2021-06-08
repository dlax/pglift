import pathlib

import pytest
from pgtoolkit.ctl import PGCtl

from pglift.util import short_version


@pytest.fixture
def datadir():
    return pathlib.Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def pg_version() -> str:
    return short_version(PGCtl().version)
