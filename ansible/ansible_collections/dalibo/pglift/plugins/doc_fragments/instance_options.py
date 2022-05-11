import sys

from ansible.module_utils.basic import missing_required_lib

try:
    import yaml

    from pglift import plugin_manager
    from pglift.models import helpers, interface
    from pglift.settings import Settings
except ImportError as e:
    print(missing_required_lib(e.name), file=sys.stderr)
    sys.exit(1)


def build_doc() -> str:
    settings = Settings()
    pm = plugin_manager(settings)
    model_type = interface.Instance.composite(pm)
    argspec = helpers.argspec_from_model(model_type)
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
