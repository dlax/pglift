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
  state:
    choices: [ present, absent ]
    default: present
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
  connection_limit:
    type: int
    required: false
    description:
      - How many concurrent connections the role can make.
  in_roles:
    type: list[str]
    required: false
    description:
      - Roles to which the new role will be added as a new member.
  inherit:
    type: bool
    required: false
    default: true
    description:
      - Let the role inherits the privileges of the roles its is a member of.
  login:
    type: bool
    required: false
    default: false
    description:
      - Allow the role to log in.
  validity:
    type: timestamp
    required: false
    description:
      - Sets a date and time after which the role's password is no longer valid.

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
from pglift import roles, types
from pglift.ansible import AnsibleContext
from pglift.models import helpers, interface, system
from pglift.pm import PluginManager


def run_module() -> None:
    model_type = interface.Role
    argspec = helpers.argspec_from_model(model_type)
    argspec["instance"] = types.AnsibleArgSpec(required=True, type="str")
    module = AnsibleModule(argument_spec=argspec, supports_check_mode=True)

    instance_id = module.params.pop("instance")
    try:
        i_name, i_version = instance_id.split("/", 1)
    except ValueError:
        i_name, i_version = instance_id, None

    try:
        role = helpers.parse_params_as(model_type, module.params)
    except pydantic.ValidationError as exc:
        module.fail_json(exc.errors())

    ctx = AnsibleContext(module, plugin_manager=PluginManager.get())

    result: Dict[str, str] = {}

    if module.check_mode:
        module.exit_json(**result)

    try:
        instance = system.Instance.system_lookup(ctx, (i_name, i_version))
        with instance_mod.running(ctx, instance):
            roles.apply(ctx, instance, role)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
