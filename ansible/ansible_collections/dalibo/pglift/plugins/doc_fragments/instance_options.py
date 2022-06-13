from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)

with check_required_libs():
    import yaml

    from pglift import plugin_manager
    from pglift.models import helpers, interface
    from pglift.settings import Settings


def build_doc() -> str:
    settings = Settings()
    pm = plugin_manager(settings)
    model_type = interface.Instance.composite(pm)
    argspec = helpers.argspec_from_model(model_type)
    return yaml.safe_dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
