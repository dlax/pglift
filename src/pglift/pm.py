from typing import Sequence

import pluggy

from . import __name__ as pkgname
from . import backup, hookspecs, pgbackrest, prometheus

hook_modules = (pgbackrest, prometheus, backup)


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

    def unregister_all(self) -> None:
        for _, plugin in self.list_name_plugin():
            self.unregister(plugin)
