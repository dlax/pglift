ANSIBLE_METADATA = {
    "metadata_version": "0.1",
    "status": ["preview"],
    "supported_by": "community",  # XXX
}

DOCUMENTATION = """
---
module: role

short_description: Create, update and delete roles of a PostgreSQL server instance

description:
- "Manage roles of a PostgreSQL server instance"

extends_documentation_fragment:
- dalibo.pglift.role_options.options
"""

EXAMPLES = """
- dalibo.pglift.role:
    instance: 12/main
    name: admin
    login: true
    superuser: true
    validity: '2023-01-01T00:00'
"""

RETURN = """
"""

from ansible.module_utils.basic import AnsibleModule
from ansible_collections.dalibo.pglift.plugins.module_utils.context import (
    AnsibleContext,
)
from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)
from ansible_collections.dalibo.pglift.plugins.module_utils.versioncheck import (
    check_pglift_version,
)

with check_required_libs():
    import pydantic

    from pglift import instances, roles, types
    from pglift.models import helpers, interface, system
    from pglift.settings import SiteSettings


def run_module() -> None:
    model_type = interface.Role
    argspec = helpers.argspec_from_model(model_type)
    argspec["instance"] = types.AnsibleArgSpec(required=True, type="str")
    module = AnsibleModule(argument_spec=argspec, supports_check_mode=True)

    check_pglift_version(module)

    instance_id = module.params.pop("instance")
    try:
        i_name, i_version = instance_id.split("/", 1)
    except ValueError:
        i_name, i_version = instance_id, None

    try:
        role = helpers.parse_params_as(model_type, module.params)
    except pydantic.ValidationError as exc:
        module.fail_json(exc.errors())

    ctx = AnsibleContext(module, settings=SiteSettings())

    result = {"changed": False}

    if module.check_mode:
        module.exit_json(**result)

    try:
        instance = system.Instance.system_lookup(ctx, (i_name, i_version))
        with instances.running(ctx, instance):
            changed = roles.apply(ctx, instance, role)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    result["changed"] = changed is not False
    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
