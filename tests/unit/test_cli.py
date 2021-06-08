import json
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from pglift import instance as instance_mod
from pglift import manifest as manifest_mod
from pglift import pgbackrest
from pglift.cli import cli
from pglift.ctx import Context


@pytest.fixture
def runner():
    return CliRunner()


def test_cli(runner):
    result = runner.invoke(cli)
    assert result.exit_code == 0


def test_instance_apply(tmp_path, runner, ctx):
    result = runner.invoke(cli, ["instance", "apply"])
    assert result.exit_code == 2
    assert "Missing option '-f'" in result.output

    manifest = tmp_path / "manifest.yml"
    content = yaml.dump({"name": "test"})
    manifest.write_text(content)
    with patch.object(instance_mod, "apply") as mock_method:
        result = runner.invoke(cli, ["instance", "apply", "-f", str(manifest)], obj=ctx)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0] == ctx
    assert isinstance(mock_method.call_args[0][1], manifest_mod.Instance)


def test_instance_schema(runner):
    result = runner.invoke(cli, ["instance", "schema"])
    schema = json.loads(result.output)
    assert schema["title"] == "Instance"
    assert schema["description"] == "PostgreSQL instance"


def test_instance_describe(runner):
    result = runner.invoke(cli, ["instance", "describe"])
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    instance = manifest_mod.Instance(name="test")
    with patch.object(instance_mod, "describe", return_value=instance) as mock_method:
        result = runner.invoke(cli, ["instance", "describe", "test"])
    mock_method.assert_called_once()
    assert isinstance(mock_method.call_args[0][0], Context)
    assert "name: test" in result.output


def test_instance_drop(runner):
    result = runner.invoke(cli, ["instance", "drop"])
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    with patch.object(instance_mod, "drop") as mock_method:
        result = runner.invoke(cli, ["instance", "drop", "test"])
    mock_method.assert_called_once()
    assert isinstance(mock_method.call_args[0][0], Context)


def test_backup_instance(runner):
    patch_backup = patch.object(pgbackrest, "backup")
    patch_expire = patch.object(pgbackrest, "expire")
    with patch_backup as backup, patch_expire as expire:
        result = runner.invoke(cli, ["backup-instance", "test", "--type=diff"])
    assert result.exit_code == 0, result.stdout
    assert backup.call_count == 1
    assert backup.call_args[1] == {"type": pgbackrest.BackupType("diff")}
    assert not expire.called

    with patch_backup as backup, patch_expire as expire:
        result = runner.invoke(cli, ["backup-instance", "test", "--purge"])
    assert result.exit_code == 0, result.stdout
    assert backup.called
    assert expire.called
