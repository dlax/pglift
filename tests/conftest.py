import pathlib
from typing import Iterator
from unittest.mock import patch

import pytest


@pytest.fixture
def datadir() -> pathlib.Path:
    return pathlib.Path(__file__).parent / "data"


@pytest.fixture(autouse=True, scope="session")
def etc(tmp_path_factory: pytest.TempPathFactory) -> Iterator[pathlib.Path]:
    etcdir = tmp_path_factory.mktemp("etc")
    with patch("pglift.util.etc", return_value=etcdir):
        yield etcdir


@pytest.fixture(autouse=True, scope="session")
def xdg_config_home(tmp_path_factory: pytest.TempPathFactory) -> Iterator[pathlib.Path]:
    config_home = tmp_path_factory.mktemp("config")
    with patch.dict("os.environ", {"XDG_CONFIG_HOME": str(config_home)}):
        yield config_home
