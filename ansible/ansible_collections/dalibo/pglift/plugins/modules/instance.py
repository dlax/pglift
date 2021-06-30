#!/usr/bin/python
# coding: utf-8

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
"""

from typing import Any, Dict

from ansible.module_utils.basic import AnsibleModule

from pglift import instance as instance_mod
from pglift import model
from pglift.ansible import AnsibleContext
from pglift.instance import Status as PGStatus
from pglift.pm import PluginManager
from pglift.settings import SETTINGS
from pglift.task import runner


def run_module() -> None:
    settings = SETTINGS
    module_args = {
        "name": {"type": "str", "required": True},
        "version": {
            "type": "str",
            "required": False,
            "choices": list(settings.postgresql.versions),
        },
        "port": {
            "type": "int",
            "required": False,
        },
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
        "configuration": {"type": "dict", "required": False},
        "prometheus_port": {"type": "int", "required": False, "default": 9187},
    }
    module = AnsibleModule(argument_spec=module_args, supports_check_mode=True)

    ctx = AnsibleContext(module, plugin_manager=PluginManager.get(), settings=settings)
    prometheus = model.PrometheusService(port=module.params["prometheus_port"])
    instance: model.BaseInstance
    if module.params["version"]:
        instance = model.InstanceSpec(
            name=module.params["name"],
            version=module.params["version"],
            settings=settings,
            prometheus=prometheus,
        )
    else:
        instance = model.InstanceSpec.default_version(
            module.params["name"], prometheus=prometheus, ctx=ctx
        )
    result = {"changed": False, "instance": str(instance)}

    if module.check_mode:
        module.exit_json(**result)

    result["state"] = state = module.params["state"]

    status = instance_mod.status(ctx, instance)
    confitems = module.params["configuration"] or {}
    if "port" in confitems:
        module.fail_json(msg="port should not be specified in configuration field")
    confitems["port"] = module.params["port"]
    ssl = module.params["ssl"] or False
    try:
        with runner(ctx):
            if state == "absent":
                if instance.exists():
                    instance = model.Instance.from_spec(instance)
                    if status == PGStatus.running:
                        instance_mod.stop(ctx, instance)
                    instance_mod.drop(ctx, instance)
                    result["changed"] = True
            else:
                result["changed"] = not instance.exists()
                instance = instance_mod.init(ctx, instance)
                result["datadir"] = str(instance.datadir)
                result["waldir"] = str(instance.waldir)
                result["configuration_changes"] = instance_mod.configure(
                    ctx, instance, ssl=ssl, **confitems
                )
                result["changed"] = result["changed"] or result["configuration_changes"]
                status = instance_mod.status(ctx, instance)
                if state == "started" and status == PGStatus.not_running:
                    instance_mod.start(ctx, instance)
                    result["changed"] = True
                elif state == "stopped" and status == PGStatus.running:
                    instance_mod.stop(ctx, instance)
                    result["changed"] = True
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
