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

options:
  name:
    type: str
    required: true
    description:
      - Unique for the service on host. Can be a reference to the target
        PostgreSQL instance, e.g. '13-main'.
  dsn:
    type: str
    required: true
    description:
      - Connection information string for target PostgreSQL instance.
  port:
    type: str
    required: true
    description:
      - TCP port for the web interface and telemetry.
  state:
    choices: [ present, absent ]
    default: present
    description:
      - Should the service be present or absent?
"""

EXAMPLES = """
- dalibo.pglift.postgres_exporter:
    name: 12-main@dbserver1
    dsn: "host=dbserver1.example.com port=5454 dbname=postgres user=monitoring"
    port: 9187
"""

RETURN = """
"""

from typing import Dict

import pydantic
from ansible.module_utils.basic import AnsibleModule

from pglift import prometheus
from pglift.ansible import AnsibleContext
from pglift.models import helpers


def run_module() -> None:
    model_type = prometheus.PostgresExporter
    argspec = helpers.argspec_from_model(model_type)
    module = AnsibleModule(argument_spec=argspec, supports_check_mode=True)

    try:
        exporter = helpers.parse_params_as(model_type, module.params)
    except pydantic.ValidationError as exc:
        module.fail_json(exc.errors())

    ctx = AnsibleContext(module)

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
