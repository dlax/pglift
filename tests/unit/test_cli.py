import json
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner
from pgtoolkit.ctl import Status

from pglift import _install, exceptions
from pglift import instance as instance_mod
from pglift import manifest as manifest_mod
from pglift import pgbackrest, roles
from pglift.cli import cli, instance_init
from pglift.ctx import Context


@pytest.fixture
def runner():
    return CliRunner()


def test_cli(runner, ctx):
    result = runner.invoke(cli, obj=ctx)
    assert result.exit_code == 0


def test_site_configure(runner, ctx, tmp_path):
    with patch.object(_install, "do") as do_install:
        result = runner.invoke(
            cli, ["site-configure", "install", f"--settings={tmp_path}"], obj=ctx
        )
    assert result.exit_code == 0, result
    do_install.assert_called_once_with(ctx, env=f"SETTINGS=@{tmp_path}")

    with patch.object(_install, "undo") as undo_install:
        result = runner.invoke(cli, ["site-configure", "uninstall"], obj=ctx)
    assert result.exit_code == 0, result
    undo_install.assert_called_once_with(ctx)


def test_instance_init(runner, ctx, instance):
    assert [p.name for p in instance_init.params] == [
        "name",
        "version",
        "port",
        "state",
        "prometheus_port",
    ]

    with patch.object(instance_mod, "apply") as apply:
        result = runner.invoke(
            cli,
            ["instance", "init", instance.name, f"--version={instance.version}"],
            obj=ctx,
        )
    assert not apply.call_count
    assert result.exit_code == 1
    assert "instance already exists" in result.stdout

    with patch.object(instance_mod, "apply") as apply:
        result = runner.invoke(
            cli,
            ["instance", "init", "new", "--port=1234"],
            obj=ctx,
        )
    apply.assert_called_once_with(ctx, manifest_mod.Instance(name="new", port=1234))
    assert result.exit_code == 0, result


def test_instance_apply(tmp_path, runner, ctx):
    result = runner.invoke(cli, ["--log-level=debug", "instance", "apply"], obj=ctx)
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


def test_instance_describe(runner, ctx, instance):
    result = runner.invoke(cli, ["instance", "describe"], obj=ctx)
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    instance = manifest_mod.Instance(name="test")
    with patch.object(instance_mod, "describe", return_value=instance) as mock_method:
        result = runner.invoke(cli, ["instance", "describe", "test"], obj=ctx)
    mock_method.assert_called_once()
    assert isinstance(mock_method.call_args[0][0], Context)
    assert "name: test" in result.output


def test_instance_list(runner, instance, ctx):
    name, version = instance.name, instance.version
    port = instance.config().port
    path = instance.path
    expected = [
        "Name Version Port Path Status",
        "-----------------------------",
        f"{name} {version} {port} {path} not_running",
    ]
    result = runner.invoke(cli, ["instance", "list"], obj=ctx)
    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0].split() == expected[0].split()
    assert lines[2].split() == expected[2].split()

    expected_list_as_json = [
        {
            "name": name,
            "path": str(path),
            "port": port,
            "status": "not_running",
            "version": version,
        }
    ]
    result = runner.invoke(cli, ["instance", "list", "--json"], obj=ctx)
    assert result.exit_code == 0
    assert json.loads(result.output) == expected_list_as_json

    result = runner.invoke(
        cli, ["instance", "list", "--json", f"--version={instance.version}"], obj=ctx
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == expected_list_as_json

    other_version = next(
        v for v in ctx.settings.postgresql.versions if v != instance.version
    )
    result = runner.invoke(
        cli, ["instance", "list", "--json", f"--version={other_version}"], obj=ctx
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == []
    result = runner.invoke(
        cli, ["instance", "list", f"--version={other_version}"], obj=ctx
    )
    assert result.exit_code == 0
    assert not result.output


def test_instance_drop(runner, ctx, instance):
    result = runner.invoke(cli, ["instance", "drop"], obj=ctx)
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    with patch.object(instance_mod, "drop") as mock_method:
        result = runner.invoke(cli, ["instance", "drop", "test"], obj=ctx)
    mock_method.assert_called_once()
    assert isinstance(mock_method.call_args[0][0], Context)


def test_instance_status(runner, instance, ctx):
    with patch.object(
        instance_mod, "status", return_value=Status.not_running
    ) as patched:
        result = runner.invoke(cli, ["instance", "status", instance.name], obj=ctx)
    assert result.exit_code == 3, result
    assert result.stdout == "not running\n"
    assert patched.call_count == 1
    args, kwargs = patched.call_args
    assert args[1] == instance
    assert kwargs == {}


@pytest.mark.parametrize(
    "action",
    ["start-instance", "stop-instance", "reload-instance", "restart-instance"],
)
def test_instance_operations(runner, instance, ctx, action):
    patched_fn = action.split("-", 1)[0]
    with patch.object(instance_mod, patched_fn) as patched:
        result = runner.invoke(cli, [action, instance.name, instance.version], obj=ctx)
    assert result.exit_code == 0, result
    assert patched.call_count == 1
    args, kwargs = patched.call_args
    assert args[1] == instance
    assert kwargs == {}


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


def test_role_schema(runner):
    result = runner.invoke(cli, ["role", "schema"])
    schema = json.loads(result.output)
    assert schema["title"] == "Role"
    assert schema["description"] == "PostgreSQL role"


def test_role_apply(runner, tmp_path, ctx, instance):
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump({"name": "roltest", "pgpass": True})
    manifest.write_text(content)
    with patch.object(roles, "apply") as apply:
        result = runner.invoke(
            cli,
            ["role", "apply", str(instance), "-f", str(manifest)],
            obj=ctx,
        )
    assert result.exit_code == 0
    apply.assert_called_once()
    (call_ctx, call_instance, call_role), kwargs = apply.call_args
    assert call_ctx == ctx
    assert call_instance == instance
    assert call_role.name == "roltest"
    assert kwargs == {}


def test_role_describe(runner, ctx, instance):
    with patch.object(
        roles, "describe", side_effect=exceptions.RoleNotFound("absent")
    ) as describe:
        result = runner.invoke(
            cli,
            ["role", "describe", str(instance), "absent"],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "absent")
    assert result.exit_code == 1
    assert result.stdout.strip() == "Error: role 'absent' not found"

    with patch.object(
        roles,
        "describe",
        return_value=manifest_mod.Role(name="present", pgpass=True, password="hidden"),
    ) as describe:
        result = runner.invoke(
            cli,
            ["role", "describe", instance.name, "present"],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "present")
    assert result.exit_code == 0
    described = yaml.safe_load(result.stdout)
    assert described == {
        "name": "present",
        "password": "**********",
        "pgpass": True,
    }


def test_role_drop(runner, ctx, instance):
    with patch.object(
        roles, "drop", side_effect=exceptions.RoleNotFound("bar")
    ) as drop:
        result = runner.invoke(
            cli,
            ["role", "drop", str(instance), "foo"],
            obj=ctx,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    assert result.exit_code == 1
    assert result.stdout.strip() == "Error: role 'bar' not found"

    with patch.object(roles, "drop") as drop:
        result = runner.invoke(
            cli,
            ["role", "drop", str(instance), "foo"],
            obj=ctx,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    assert result.exit_code == 0
