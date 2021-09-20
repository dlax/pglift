import pathlib

import pluggy
from typing_extensions import Final

__all__ = ["hookimpl"]

hookimpl = pluggy.HookimplMarker(__name__)


datapath = pathlib.Path(__file__).parent / "data"

prometheus_default_port: Final = 9187


def template(*args: str) -> str:
    return datapath.joinpath(*args).read_text()
