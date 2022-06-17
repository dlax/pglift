from ansible.module_utils.basic import AnsibleModule
from ansible_collections.dalibo.pglift.plugins.module_utils.importcheck import (
    check_required_libs,
)

PGLIFT_REQUIRED_MIN_VERSION = "0.22.1"

with check_required_libs():
    from pglift import version as pglift_version

try:
    from ansible.module_utils.compat.version import LooseVersion as Version
except ImportError:
    with check_required_libs():
        from packaging.version import Version


def check_pglift_version(module: AnsibleModule) -> None:
    if Version(pglift_version()) < Version(PGLIFT_REQUIRED_MIN_VERSION):
        module.fail_json(
            msg=f"Version of pglift lower than recommended version ({PGLIFT_REQUIRED_MIN_VERSION})"
        )
