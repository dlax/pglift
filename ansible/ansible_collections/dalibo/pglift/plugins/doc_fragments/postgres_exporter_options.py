from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)

with check_required_libs():
    import yaml

    from pglift import prometheus
    from pglift.models import helpers


def build_doc() -> str:
    model_type = prometheus.PostgresExporter
    argspec = helpers.argspec_from_model(model_type)
    return yaml.dump({"options": argspec}, sort_keys=False)  # type: ignore[no-any-return]


class ModuleDocFragment(object):
    OPTIONS = build_doc()
