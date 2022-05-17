from typing import TYPE_CHECKING, Optional

from .. import databases, instances, roles, types, util
from ..models import interface, system
from ..task import task

if TYPE_CHECKING:
    from ..ctx import BaseContext
    from ..settings import PowaSettings
    from .models import ServiceManifest

# Extensions to load in shared_preload_libraries and to install.
# Order is important here, for example`pg_stat_statements` needs to be loaded
# before `pg_stat_kcache` in shared_preload_libraries
POWA_EXTENSIONS = [
    types.Extension.btree_gist,
    types.Extension.pg_qualstats,
    types.Extension.pg_stat_statements,
    types.Extension.pg_stat_kcache,
    types.Extension.powa,
]


def available(ctx: "BaseContext") -> Optional["PowaSettings"]:
    return ctx.settings.powa


@task("setting up PoWA database and role")
def setup_db(
    ctx: "BaseContext",
    instance: system.PostgreSQLInstance,
    manifest: "ServiceManifest",
    settings: "PowaSettings",
) -> None:
    if instance.standby:
        return
    with instances.running(ctx, instance):
        password = manifest.password or util.generate_password()
        role = interface.Role(
            name=settings.role,
            password=password,
            login=True,
            superuser=True,
        )
        if not roles.exists(ctx, instance, role.name):
            roles.create(ctx, instance, role)

        database = interface.Database(name=settings.dbname, extensions=POWA_EXTENSIONS)
        if not databases.exists(ctx, instance, database.name):
            databases.create(ctx, instance, database)
