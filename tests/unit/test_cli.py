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


def test_cli(runner, ctx):
    result = runner.invoke(cli, obj=ctx)
    assert result.exit_code == 0


def test_instance_apply(tmp_path, runner, ctx):
    result = runner.invoke(cli, ["instance", "apply"], obj=ctx)
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


def test_instance_schema(runner, ctx):
    result = runner.invoke(cli, ["instance", "schema"], obj=ctx)
    schema = json.loads(result.output)
    assert schema["title"] == "Instance"
    assert schema["description"] == "PostgreSQL instance"


def test_instance_describe(runner, ctx):
    result = runner.invoke(cli, ["instance", "describe"], obj=ctx)
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    instance = manifest_mod.Instance(name="test")
    with patch.object(instance_mod, "describe", return_value=instance) as mock_method:
        result = runner.invoke(cli, ["instance", "describe", "test"], obj=ctx)
    mock_method.assert_called_once()
    assert isinstance(mock_method.call_args[0][0], Context)
    assert "name: test" in result.output


def test_instance_drop(runner, ctx):
    result = runner.invoke(cli, ["instance", "drop"], obj=ctx)
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    with patch.object(instance_mod, "drop") as mock_method:
        result = runner.invoke(cli, ["instance", "drop", "test"], obj=ctx)
    mock_method.assert_called_once()
    assert isinstance(mock_method.call_args[0][0], Context)


def test_backup_instance(runner, instance, ctx):
    patch_backup = patch.object(pgbackrest, "backup")
    patch_expire = patch.object(pgbackrest, "expire")
    with patch_backup as backup, patch_expire as expire:
        result = runner.invoke(
            cli,
            ["backup-instance", instance.name, instance.version, "--type=diff"],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    assert backup.call_count == 1
    assert backup.call_args[1] == {"type": pgbackrest.BackupType("diff")}
    assert not expire.called

    with patch_backup as backup, patch_expire as expire:
        result = runner.invoke(
            cli,
            ["backup-instance", instance.name, instance.version, "--purge"],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    assert backup.called
    assert expire.called
