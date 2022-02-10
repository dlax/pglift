from typing import TYPE_CHECKING

import pluggy
from typing_extensions import Final

from . import _compat, pm, settings
from .models import interface

__all__ = ["hookimpl"]

hookimpl = pluggy.HookimplMarker(__name__)


def version() -> str:
    return _compat.version(__name__)


SETTINGS: Final = settings.Settings()
PLUGIN_MANAGER: Final = pm.PluginManager.get()

if not TYPE_CHECKING:
    CompositeInstance = interface.Instance.composite(PLUGIN_MANAGER)
else:
    # Mypy cannot handle dynamic base class so we lie when type-checking
    # (https://github.com/python/mypy/issues/2477)
    CompositeInstance = interface.Instance
