import pathlib

import pluggy

__all__ = ["hookimpl"]

hookimpl = pluggy.HookimplMarker(__name__)


datapath = pathlib.Path(__file__).parent / "data"


def template(*args: str) -> str:
    return datapath.joinpath(*args).read_text()
