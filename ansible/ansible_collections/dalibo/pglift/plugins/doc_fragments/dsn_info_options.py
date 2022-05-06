import yaml
from ansible.errors import AnsibleError

try:
    from pglift.models import helpers, interface
except ImportError:
    raise AnsibleError("pglift must be installed to use this plugin")


def build_doc() -> str:
    argspec = helpers.argspec_from_model(interface.BaseInstance)
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
