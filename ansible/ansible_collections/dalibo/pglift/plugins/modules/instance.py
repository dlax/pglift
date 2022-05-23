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
env:
    description: libpq environment variable used to connect to the instance
"""

from ansible.module_utils.basic import AnsibleModule
from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)

with check_required_libs():
    import pydantic

    from pglift import exceptions, instances, plugin_manager
    from pglift.ansible import AnsibleContext
    from pglift.models import helpers, interface, system
    from pglift.settings import SiteSettings


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

    result = {"changed": False, "instance": str(m), "env": {}}

    if module.check_mode:
        module.exit_json(**result)

    try:
        changed = instances.apply(ctx, m)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    result["changed"] = changed is not False

    if changed is not None:
        try:
            instance = system.PostgreSQLInstance.system_lookup(ctx, (m.name, m.version))
        except exceptions.InstanceNotFound:
            # Instance probably got dropped in a previous task.
            pass
        else:
            result["env"] = instances.env_for(ctx, instance, path=True)

    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
