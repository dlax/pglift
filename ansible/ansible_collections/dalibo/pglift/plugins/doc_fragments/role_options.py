from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)

with check_required_libs():
    import yaml

    from pglift import types
    from pglift.models import helpers, interface


def build_doc() -> str:
    model_type = interface.Role
    argspec = helpers.argspec_from_model(model_type)
    argspec["instance"] = types.AnsibleArgSpec(
        required=True, type="str", description=["Instance name."]
    )
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
