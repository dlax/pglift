import pathlib

import pytest


@pytest.fixture
def datadir() -> pathlib.Path:
    return pathlib.Path(__file__).parent / "data"
