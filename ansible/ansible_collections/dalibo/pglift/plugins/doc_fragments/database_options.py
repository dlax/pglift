import yaml
from ansible.errors import AnsibleError

try:
    from pglift import types
    from pglift.models import helpers, interface
except ImportError:
    raise AnsibleError("pglift must be installed to use this plugin")


def build_doc() -> str:
    model_type = interface.Database
    argspec = helpers.argspec_from_model(model_type)
    argspec["instance"] = types.AnsibleArgSpec(
        required=True, type="str", description=["Instance name."]
    )
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
