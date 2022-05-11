import sys

from ansible.module_utils.basic import missing_required_lib

try:
    import yaml

    from pglift import types
    from pglift.models import helpers, interface
except ImportError as e:
    print(missing_required_lib(e.name), file=sys.stderr)
    sys.exit(1)


def build_doc() -> str:
    model_type = interface.Database
    argspec = helpers.argspec_from_model(model_type)
    argspec["instance"] = types.AnsibleArgSpec(
        required=True, type="str", description=["Instance name."]
    )
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
