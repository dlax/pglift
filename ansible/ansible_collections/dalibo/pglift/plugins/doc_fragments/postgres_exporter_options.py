import yaml
from ansible.errors import AnsibleError

try:
    from pglift import prometheus
    from pglift.models import helpers
except ImportError:
    raise AnsibleError("pglift must be installed to use this plugin")


def build_doc() -> str:
    model_type = prometheus.PostgresExporter
    argspec = helpers.argspec_from_model(model_type)
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
