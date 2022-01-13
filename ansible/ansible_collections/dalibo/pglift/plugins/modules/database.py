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
      - Database name.
  state:
    choices: [ present, absent ]
    default: present
  owner:
    type: str
    description:
      - The role name of the user who will own the new database.

author:
- Dalibo (@dalibo)
"""

EXAMPLES = """
- dalibo.pglift.database:
    instance: 12/main
    name: db
"""

RETURN = """
"""

from typing import Dict

import pydantic
from ansible.module_utils.basic import AnsibleModule

from pglift import databases
from pglift import instance as instance_mod
from pglift import types
from pglift.ansible import AnsibleContext
from pglift.models import helpers, interface, system
from pglift.pm import PluginManager
from pglift.task import Runner


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

    ctx = AnsibleContext(module, plugin_manager=PluginManager.get())

    result: Dict[str, str] = {}

    if module.check_mode:
        module.exit_json(**result)

    try:
        instance = system.Instance.system_lookup(ctx, (i_name, i_version))
        with Runner(), instance_mod.running(ctx, instance):
            databases.apply(ctx, instance, database)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    module.exit_json(**result)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
