ANSIBLE_METADATA = {
    "metadata_version": "0.1",
    "status": ["preview"],
    "supported_by": "community",  # XXX
}

DOCUMENTATION = """
---
module: postgres_exporter

short_description: Manage Prometheus postgres_exporter for a PostgreSQL instance.

description:
- "Manage Prometheus postgres_exporter for a PostgreSQL instance"

extends_documentation_fragment:
- dalibo.pglift.postgres_exporter_options.options
"""

EXAMPLES = """
- dalibo.pglift.postgres_exporter:
    name: 12-main@dbserver1
    dsn: "host=dbserver1.example.com port=5454 dbname=postgres user=monitoring"
    port: 9187
"""

RETURN = """
"""
import sys
from typing import Dict

import pydantic
from ansible.module_utils.basic import AnsibleModule, missing_required_lib

try:
    from pglift import prometheus
    from pglift.ansible import AnsibleContext
    from pglift.models import helpers
    from pglift.settings import SiteSettings
except ImportError as e:
    print(missing_required_lib(e.name), file=sys.stderr)
    sys.exit(1)


def run_module() -> None:
    model_type = prometheus.PostgresExporter
    argspec = helpers.argspec_from_model(model_type)
    module = AnsibleModule(argument_spec=argspec, supports_check_mode=True)

    try:
        exporter = helpers.parse_params_as(model_type, module.params)
    except pydantic.ValidationError as exc:
        module.fail_json(exc.errors())

    ctx = AnsibleContext(module, settings=SiteSettings())

    result: Dict[str, str] = {}

    if module.check_mode:
        module.exit_json(**result)

    if ctx.settings.prometheus is None:
        raise RuntimeError("prometheus is disabled in site settings")
    try:
        prometheus.apply(ctx, exporter, ctx.settings.prometheus)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
