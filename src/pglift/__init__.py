import pluggy
from typing_extensions import Final

from . import _compat, settings

__all__ = ["hookimpl"]

hookimpl = pluggy.HookimplMarker(__name__)

prometheus_default_port: Final = 9187


def version() -> str:
    return _compat.version(__name__)


SETTINGS: Final = settings.Settings()
