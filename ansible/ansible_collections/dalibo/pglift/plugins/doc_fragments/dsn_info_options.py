from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)

with check_required_libs():
    import yaml

    from pglift.models import helpers, interface


def build_doc() -> str:
    argspec = helpers.argspec_from_model(interface.BaseInstance)
    return yaml.safe_dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
