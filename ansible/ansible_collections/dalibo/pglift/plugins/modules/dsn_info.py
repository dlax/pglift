ANSIBLE_METADATA = {
    "metadata_version": "0.1",
    "status": ["preview"],
    "supported_by": "community",  # XXX
}

DOCUMENTATION = """
---
module: dsn_info

short_description: get libpq environment variables to connect to a PostgreSQL server instance

description:
- "Get libpq environment variables to connect to a PostgreSQL server instance"

extends_documentation_fragment:
- dalibo.pglift.dsn_info_options.options

author:
- Dalibo (@dalibo)
"""

EXAMPLES = """
- name: Register production DB instance conninfo
  dalibo.pglift.dsn_info:
    name: prod
  register: prod

- name: Create database testdb
  community.postgresql.postgresql_db:
    name: testdb
    login_host: "{{ primary.PGHOST }}"
    port: "{{ primary.PGPORT }}"
  environment: "{{ prod }}"
"""

RETURN = """
PGHOST:
  description: instance host or socket directory
  type: str
  returned: always
PGPORT:
  description: instance port
  type: str
  returned: always
PGUSER:
  description: instance user
  type: str
  returned: always
PGPASSFILE:
  description: instance passfile
  type: str
  returned: always
PATH:
  description: original PATH updated with PostgreSQL version binaries path
  type: str
  returned: always
"""

import sys

from ansible.module_utils.basic import AnsibleModule, missing_required_lib

try:
    from pglift import instances
    from pglift.ansible import AnsibleContext
    from pglift.models import helpers, interface, system
    from pglift.settings import SiteSettings
except ImportError as e:
    print(missing_required_lib(e.name), file=sys.stderr)
    sys.exit(1)


def run_module() -> None:
    argspec = helpers.argspec_from_model(interface.BaseInstance)
    module = AnsibleModule(argspec, supports_check_mode=True)
    settings = SiteSettings()
    ctx = AnsibleContext(module, settings=settings)
    instance = system.PostgreSQLInstance.system_lookup(
        ctx, (module.params["name"], module.params["version"])
    )
    env = instances.env_for(ctx, instance, path=True)
    module.exit_json(**env)


def main() -> None:
    run_module()


if __name__ == "__main__":
    main()
