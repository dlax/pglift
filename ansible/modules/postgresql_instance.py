#!/usr/bin/python
# coding: utf-8

ANSIBLE_METADATA = {
    "metadata_version": "0.1",
    "status": ["preview"],
    "supported_by": "community",  # XXX
}

DOCUMENTATION = """
---
module: postgresql_instance

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
      - PostgreSQL version of the instance
    type: str
    required: true
  init_options:
    description:
      - Options passed to initdb (e.g. data_checksums)
    type: dict
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

author:
- Dalibo (@dalibo)
"""

EXAMPLES = """
# A PostgreSQL instance with SSL and a custom certificates:
- name: Production DB instance
  postgresql_instance:
    name: prod
    version: 11
    ssl:
      - /etc/certs/db.cert
      - /etc/certs/db.key
    init_options:
      data_checksums: true
      locale: fr_FR.UTF-8
    configuration:
      listen_addresses: "*"
      shared_buffers: "1GB"

# A PostgreSQL instance with a generated self-signed certificate, not started:
- name: Pre-production DB instance
  postgresql_instance:
    name: preprod
    version: 12
    state: stopped
    ssl: true

# A PostgreSQL instance without SSL and with some custom settings:
- name: Dev DB instance
  postgresql_instance:
    name: dev
    version: 13
    configuration:
      max_connections: 10
      log_statement: 'all'
"""

RETURN = """
instance:
  description: Fully qualified name of the instance
  type: str
  returned: always
datadir:
  description: Path to PostgreSQL server instance data directory
  type: str
  returned: always
waldir:
  description: Path to PostgreSQL server instance WAL directory
  type: str
  returned: always
configuration_changes:
  description: Changes to PostgreSQL instance configuration
  type: dict
  returned: always
started:
  description: Whether or not the instance got started
  type: bool
  returned: always
"""

from typing import Any, Dict

from ansible.module_utils.basic import AnsibleModule

from pglib import instance as instance_mod
from pglib.ansible import ansible_runner
from pglib.model import Instance
from pglib.pg import Status as PGStatus


def run_module() -> None:
    module_args = {
        "name": {"type": "str", "required": True},
        "version": {"type": "str", "required": True},
        "state": {
            "type": "str",
            "choices": ["started", "stopped", "absent"],
            "default": "started",
        },
        "ssl": {
            "type": "bool",  # XXX or type: list
            "required": False,
            "default": False,
        },
        "init_options": {"type": "dict", "required": False},
        "configuration": {"type": "dict", "required": False},
    }
    module = AnsibleModule(argument_spec=module_args, supports_check_mode=True)

    instance = Instance(module.params["name"], module.params["version"])
    result = {"changed": False, "instance": str(instance)}

    if module.check_mode:
        module.exit_json(**result)

    result["state"] = state = module.params["state"]

    run_command = ansible_runner(module)
    status = instance_mod.status(instance, run_command=run_command)
    init_options = module.params["init_options"] or {}
    confitems = module.params["configuration"] or {}
    ssl = module.params["ssl"] or False
    try:
        if state == "absent" and instance.exists():
            if status == PGStatus.RUNNING:
                instance_mod.stop(instance, run_command=run_command)
            instance_mod.revert_configure(instance, run_command=run_command)
            instance_mod.revert_init(instance, run_command=run_command)
        else:
            result["changed"] = instance_mod.init(
                instance, run_command=run_command, **init_options
            )
            result["datadir"] = str(instance.datadir)
            result["waldir"] = str(instance.waldir)
            result["configuration_changes"] = instance_mod.configure(
                instance, ssl=ssl, **confitems
            )
            result["changed"] = result["changed"] or result["configuration_changes"]
            status = instance_mod.status(instance, run_command=run_command)
            if state == "started" and status == PGStatus.NOT_RUNNING:
                instance_mod.start(instance, run_command=run_command)
            elif state == "stopped" and status == PGStatus.RUNNING:
                instance_mod.stop(instance, run_command=run_command)
    except Exception as exc:
        module.fail_json(msg=f"Error {exc}", **result)

    if module._diff:
        diff: Dict[str, Any] = {}
        before = diff["before"] = {}
        after = diff["after"] = {}
        # TODO: use configuration file path instead
        diff["before_header"] = diff["after_header"] = str(instance)
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