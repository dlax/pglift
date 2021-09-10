import pathlib

import pytest


@pytest.fixture
def datadir():
    return pathlib.Path(__file__).parent / "data"
