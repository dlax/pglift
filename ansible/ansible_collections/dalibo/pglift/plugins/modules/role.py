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

options:
  instance:
    type: str
    required: true
    description:
      - Instance fully qualified identifier as <version>/<name> or <name>,
        when the version is to be guessed.
  name:
    type: str
    required: true
    description:
      - Role name.
  password:
    type: str
    required: false
    description:
      - Role password.
  pgpass:
    type: bool
    required: false
    description:
      - If True, add an entry in password file for this role.

author:
- Dalibo (@dalibo)
"""

EXAMPLES = """
- dalibo.pglift.role:
    instance: 12/main
    name: admin
    pgpass: true
"""

RETURN = """
"""

from typing import Dict

import pydantic
from ansible.module_utils.basic import AnsibleModule

from pglift import instance as instance_mod
from pglift import roles
from pglift.ansible import AnsibleContext
from pglift.models import helpers, interface, system
from pglift.pm import PluginManager
from pglift.settings import SETTINGS
from pglift.task import runner


def run_module() -> None:
    settings = SETTINGS
    model_type = interface.Role
    argspec = helpers.argspec_from_model(model_type)
    argspec["instance"] = helpers.ArgSpec(required=True, type="str")
    module = AnsibleModule(argument_spec=argspec, supports_check_mode=True)

    instance_id = module.params.pop("instance")
    try:
        i_name, i_version = instance_id.split("/", 1)
    except ValueError:
        i_name, i_version = instance_id, None

    try:
        role = helpers.parse_params_as(model_type, module.params)
    except pydantic.ValidationError as exc:
        module.fail_json("; ".join(e["msg"] for e in exc.errors()))

    ctx = AnsibleContext(module, plugin_manager=PluginManager.get(), settings=settings)

    result: Dict[str, str] = {}

    if module.check_mode:
        module.exit_json(**result)

    try:
        instance = system.Instance.system_lookup(ctx, (i_name, i_version))
        with runner(ctx), instance_mod.running(ctx, instance):
            roles.apply(ctx, instance, role)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
