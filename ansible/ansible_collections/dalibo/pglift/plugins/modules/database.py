ANSIBLE_METADATA = {
    "metadata_version": "0.1",
    "status": ["preview"],
    "supported_by": "community",  # XXX
}

DOCUMENTATION = """
---
module: database

short_description: Create, update and delete databases of a PostgreSQL server instance

description:
- "Manage databases of a PostgreSQL server instance"

extends_documentation_fragment:
- dalibo.pglift.database_options.options
"""

EXAMPLES = """
- dalibo.pglift.database:
    instance: 12/main
    name: db
    settings:
      work_mem: 2MB
"""

RETURN = """
"""
import sys
from typing import Dict

from ansible.module_utils.basic import AnsibleModule, missing_required_lib

try:
    import pydantic

    from pglift import databases, instances, types
    from pglift.ansible import AnsibleContext
    from pglift.models import helpers, interface, system
    from pglift.settings import SiteSettings
except ImportError as e:
    print(missing_required_lib(e.name), file=sys.stderr)
    sys.exit(1)


def run_module() -> None:
    model_type = interface.Database
    argspec = helpers.argspec_from_model(model_type)
    argspec["instance"] = types.AnsibleArgSpec(required=True, type="str")
    module = AnsibleModule(argument_spec=argspec, supports_check_mode=True)

    instance_id = module.params.pop("instance")
    try:
        i_name, i_version = instance_id.split("/", 1)
    except ValueError:
        i_name, i_version = instance_id, None

    try:
        database = helpers.parse_params_as(model_type, module.params)
    except pydantic.ValidationError as exc:
        module.fail_json(exc.errors())

    ctx = AnsibleContext(module, settings=SiteSettings())

    result: Dict[str, str] = {}

    if module.check_mode:
        module.exit_json(**result)

    try:
        instance = system.Instance.system_lookup(ctx, (i_name, i_version))
        with instances.running(ctx, instance):
            databases.apply(ctx, instance, database)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
