ANSIBLE_METADATA = {
    "metadata_version": "0.1",
    "status": ["preview"],
    "supported_by": "community",  # XXX
}

DOCUMENTATION = """
---
module: instance

short_description: Create, update and delete a PostgreSQL server instance

description:
- "Manage a PostgreSQL server instance"

options:
  name:
    description:
      - Name of the instance
    type: str
    required: true
  version:
    description:
      - PostgreSQL version of the instance.
      - If not set, version is guessed from installed PostgreSQL.
    type: str
    required: false
  port:
    description:
      - TCP port the PostgreSQL instance will be listening on.
    type: int
    required: false
  state:
    description:
      - instance state
    choices: [ started, stopped, absent ]
  ssl:
    description:
      - Enable SSL.
      - If True, enable SSL and generated a self-signed certificate.
      - If a list of [cert_file, key_file], enable SSL and use given
        certificate.
    type: bool or list of strings
    required: false
  configuration:
    description:
      - Settings for the PostgreSQL instance.
    type: dict
    required: false
  prometheus_port:
    description:
      - TCP port for the web interface and telemetry of Prometheus
      - postgres_exporter
    type: int
    required: true
    default: 9187
  standby_for:
    description:
      - DSN of primary for streaming replication
    type: str
  standby_status:
    description:
      - Instance standby state
    choices: [ demoted, promoted ]
    default: demoted
  extensions:
    description:
      - Extensions to install in the instance
    type: list[str]
    required: false

author:
- Dalibo (@dalibo)
"""

EXAMPLES = """
# A PostgreSQL instance with SSL and a custom certificates:
- name: Production DB instance
  dalibo.pglift.instance:
    name: prod
    ssl:
      - /etc/certs/db.cert
      - /etc/certs/db.key
    configuration:
      listen_addresses: "*"
      shared_buffers: "1GB"

# A PostgreSQL instance with a generated self-signed certificate, not started:
- name: Pre-production DB instance
  dalibo.pglift.instance:
    name: preprod
    version: 12
    state: stopped
    ssl: true
    prometheus_port: 9188

# A PostgreSQL instance without SSL and with some custom settings:
- name: Dev DB instance
  dalibo.pglift.instance:
    name: dev
    version: 13
    port: 5455
    configuration:
      max_connections: 10
      log_statement: 'all'
"""

RETURN = """
instance:
  description: Fully qualified name of the instance
  type: str
  returned: always
configuration_changes:
  description: Changes to PostgreSQL instance configuration
  type: dict
needs_restart:
  description: Whether the instance needs to be restarted or not
  type: bool
"""

from typing import Any, Dict

import pydantic
from ansible.module_utils.basic import AnsibleModule

from pglift import instances, plugin_manager
from pglift.ansible import AnsibleContext
from pglift.models import helpers, interface
from pglift.settings import Settings


def run_module() -> None:
    settings = Settings()
    pm = plugin_manager(settings)
    model_type = interface.Instance.composite(pm)
    argspec = helpers.argspec_from_model(model_type)
    module = AnsibleModule(argument_spec=argspec, supports_check_mode=True)

    try:
        m = helpers.parse_params_as(model_type, module.params)
    except pydantic.ValidationError as exc:
        module.fail_json(exc.errors())

    ctx = AnsibleContext(module, settings=settings)
    assert ctx.pm == pm, f"inconsistent plugin manager used by {ctx} and argspec ({pm})"

    result = {"changed": False, "instance": str(m)}

    if module.check_mode:
        module.exit_json(**result)

    instance_exists = instances.exists(ctx, m.name, m.version)

    try:
        apply_result = instances.apply(ctx, m)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    if instance_exists:
        if not apply_result:  # Dropped
            result["changed"] = True
            instance = None
        else:
            instance, changes, needs_restart = apply_result
            if changes:
                result["changed"] = True
                result["configuration_changes"] = changes
                result["needs_restart"] = needs_restart
    elif apply_result:  # Created
        instance, changes, needs_restart = apply_result
        result["changed"] = True
        result["configuration_changes"] = changes
        result["needs_restart"] = needs_restart
    else:
        instance = None

    if module._diff:
        diff: Dict[str, Any] = {}
        before = diff["before"] = {}
        after = diff["after"] = {}
        # TODO: use configuration file path instead
        diff["before_header"] = diff["after_header"] = str(instance or m)
        for k, (before_val, after_val) in result.get(  # type: ignore[attr-defined]
            "configuration_changes", {}
        ).items():
            before[k] = before_val
            after[k] = after_val
        result["diff"] = diff

    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
