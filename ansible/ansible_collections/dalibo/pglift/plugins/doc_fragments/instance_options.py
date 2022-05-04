import yaml
from ansible.errors import AnsibleError

try:
    from pglift import plugin_manager
    from pglift.models import helpers, interface
    from pglift.settings import Settings
except ImportError:
    raise AnsibleError("pglift must be installed to use this plugin")


def build_doc() -> str:
    settings = Settings()
    pm = plugin_manager(settings)
    model_type = interface.Instance.composite(pm)
    argspec = helpers.argspec_from_model(model_type)
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
