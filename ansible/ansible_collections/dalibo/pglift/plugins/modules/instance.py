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
extends_documentation_fragment:
- dalibo.pglift.instance_options.options

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
from ansible.errors import AnsibleError
from ansible.module_utils.basic import AnsibleModule

try:
    from pglift import instances, plugin_manager
    from pglift.ansible import AnsibleContext
    from pglift.models import helpers, interface
    from pglift.settings import SiteSettings
except ImportError:
    raise AnsibleError("pglift must be installed to use this plugin")


def run_module() -> None:
    settings = SiteSettings()
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
