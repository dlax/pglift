import pluggy
from typing_extensions import Final

from . import _compat, pm, settings

__all__ = ["hookimpl"]

hookimpl = pluggy.HookimplMarker(__name__)


def version() -> str:
    return _compat.version(__name__)


SETTINGS: Final = settings.Settings()
PLUGIN_MANAGER: Final = pm.PluginManager.get()
