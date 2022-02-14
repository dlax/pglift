import pluggy

from . import _compat, pm, settings

__all__ = ["hookimpl"]

hookimpl = pluggy.HookimplMarker(__name__)


def version() -> str:
    return _compat.version(__name__)


def plugin_manager(s: settings.Settings) -> pm.PluginManager:
    disabled_plugins = [
        name for name, plugin_settings in settings.plugins(s) if plugin_settings is None
    ]
    return pm.PluginManager.get(no_register=disabled_plugins)
