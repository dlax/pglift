from types import ModuleType
from typing import List, Sequence

import pluggy

from . import __name__ as pkgname
from . import backup, hookspecs, instance, pgbackrest, prometheus, roles

hook_modules = (backup, instance, pgbackrest, prometheus, roles)


class PluginManager(pluggy.PluginManager):  # type: ignore[misc]
    @classmethod
    def get(cls, no_register: Sequence[str] = ()) -> "PluginManager":
        self = cls(pkgname)
        no_register = tuple(f"{pkgname}.{n}" for n in no_register)
        self.add_hookspecs(hookspecs)
        for hm in hook_modules:
            if hm.__name__ not in no_register:
                self.register(hm)
        return self

    def unregister_all(self) -> List[ModuleType]:
        unregistered = []
        for __, plugin in self.list_name_plugin():
            self.unregister(plugin)
            unregistered.append(plugin)
        return unregistered
