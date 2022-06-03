import json
from pathlib import Path
from typing import Type

import pytest

from pglift import pm, prometheus
from pglift.models import helpers, interface
from pglift.types import Manifest

# Composite instance model with all plugins enabled, for Ansible argspec tests.
composite_instance_model = interface.Instance.composite(pm.PluginManager.get())


@pytest.mark.parametrize(
    "manifest_type",
    [
        composite_instance_model,
        prometheus.PostgresExporter,
        interface.Role,
        interface.Database,
    ],
)
def test_argspec_from_model_manifest(
    datadir: Path, write_changes: bool, manifest_type: Type[Manifest]
) -> None:
    actual = helpers.argspec_from_model(manifest_type)
    fpath = datadir / f"ansible-argspec-{manifest_type.__name__.lower()}.json"
    if write_changes:
        fpath.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
    expected = json.loads(fpath.read_text())
    assert actual == expected
