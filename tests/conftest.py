import pathlib
from typing import Callable, Iterator, Optional
from unittest.mock import patch

import pytest


@pytest.fixture
def datadir() -> pathlib.Path:
    return pathlib.Path(__file__).parent / "data"


@pytest.fixture(scope="session", autouse=True)
def site_config() -> Iterator[Callable[..., Optional[pathlib.Path]]]:
    """Avoid looking up for configuration files in site directories, fall back
    to distribution one.
    """
    from pglift import util

    with patch("pglift.util.site_config", new=util.dist_config) as fn:
        yield fn
