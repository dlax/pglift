import importlib
from types import ModuleType
from typing import Any, List, Sequence

import pluggy

from . import __name__ as pkgname
from . import hookspecs


class PluginManager(pluggy.PluginManager):  # type: ignore[misc]
    @classmethod
    def get(cls, no_register: Sequence[str] = ()) -> "PluginManager":
        hook_modules = ("instance", "passfile", "backup", "pgbackrest", "prometheus")
        self = cls(pkgname)
        self.add_hookspecs(hookspecs)
        for hname in hook_modules:
            if hname not in no_register:
                hm = importlib.import_module(f"{pkgname}.{hname}")
                self.register(hm)
        return self

    def unregister_all(self) -> List[ModuleType]:
        unregistered = []
        for __, plugin in self.list_name_plugin():
            self.unregister(plugin)
            unregistered.append(plugin)
        return unregistered

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.get_plugins() == other.get_plugins()  # type: ignore[no-any-return]
