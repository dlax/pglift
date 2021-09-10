import datetime
import json
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner
from pgtoolkit.ctl import Status

from pglift import _install, databases, exceptions
from pglift import instance as instance_mod
from pglift import pgbackrest, prometheus, roles
from pglift.cli import cli, instance_init
from pglift.ctx import Context
from pglift.models import interface
from pglift.models.system import Instance


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def running(ctx: Context, instance: Instance) -> Iterator[MagicMock]:
    with patch("pglift.instance.running") as m:
        yield m
    m.assert_called_once_with(ctx, instance)


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
        "standby_for",
        "standby_slot",
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
    apply.assert_called_once_with(ctx, interface.Instance(name="new", port=1234))
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
    assert result.exit_code == 0, (result, result.output)
    mock_method.assert_called_once()
    assert mock_method.call_args[0][0] == ctx
    assert isinstance(mock_method.call_args[0][1], interface.Instance)


def test_instance_alter(runner, ctx):
    result = runner.invoke(
        cli, ["instance", "alter", "notfound", "--version=11"], obj=ctx
    )
    assert result.exit_code == 1
    assert "Error: instance '11/notfound' not found" in result.output

    actual = interface.Instance.parse_obj(
        {"name": "alterme", "prometheus": {"port": 1212}}
    )
    altered = interface.Instance.parse_obj(
        {
            "name": "alterme",
            "state": "stopped",
            "prometheus": {"port": 2121},
        }
    )
    with patch.object(instance_mod, "apply") as apply, patch.object(
        instance_mod, "describe", return_value=actual
    ) as describe:
        result = runner.invoke(
            cli,
            [
                "instance",
                "alter",
                "alterme",
                "--state=stopped",
                "--prometheus-port=2121",
            ],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, "alterme", None)
    apply.assert_called_once_with(ctx, altered)
    assert result.exit_code == 0, result.output


def test_instance_schema(runner, ctx):
    result = runner.invoke(cli, ["instance", "schema"], obj=ctx)
    schema = json.loads(result.output)
    assert schema["title"] == "Instance"
    assert schema["description"] == "PostgreSQL instance"


def test_instance_describe(runner, ctx, instance):
    result = runner.invoke(cli, ["instance", "describe"], obj=ctx)
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    instance = interface.Instance(name="test")
    with patch.object(instance_mod, "describe", return_value=instance) as describe:
        result = runner.invoke(cli, ["instance", "describe", "test"], obj=ctx)
    assert result.exit_code == 0, (result, result.output)
    describe.assert_called_once_with(ctx, "test", None)
    assert "name: test" in result.output


def test_instance_list(runner, instance, ctx):
    name, version = instance.name, instance.version
    port = instance.config().port
    path = instance.path
    expected = [
        "name version port path status",
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
    assert not result.output.strip()


def test_instance_drop(runner, ctx, instance):
    result = runner.invoke(cli, ["instance", "drop"], obj=ctx)
    assert result.exit_code == 2
    assert "Missing argument 'NAME'" in result.output

    with patch.object(instance_mod, "drop") as mock_method:
        result = runner.invoke(cli, ["instance", "drop", "test"], obj=ctx)
    assert result.exit_code == 0, (result, result.output)
    mock_method.assert_called_once()
    assert isinstance(mock_method.call_args[0][0], Context)


def test_instance_status(runner, instance, ctx):
    with patch.object(
        instance_mod, "status", return_value=Status.not_running
    ) as patched:
        result = runner.invoke(cli, ["instance", "status", instance.name], obj=ctx)
    assert result.exit_code == 3, (result, result.output)
    assert result.stdout == "not running\n"
    assert patched.call_count == 1
    args, kwargs = patched.call_args
    assert args[1].name == instance.name
    assert kwargs == {}


@pytest.mark.parametrize(
    "action",
    ["start", "stop", "reload", "restart"],
)
def test_instance_operations(runner, instance, ctx, action):
    with patch.object(instance_mod, action) as patched:
        result = runner.invoke(
            cli, ["instance", action, instance.name, instance.version], obj=ctx
        )
    assert result.exit_code == 0, result
    assert patched.call_count == 1
    args, kwargs = patched.call_args
    assert args[1] == instance
    assert kwargs == {}


def test_instance_shell(runner, instance, ctx):
    with patch.object(
        instance_mod, "status", return_value=instance_mod.Status.not_running
    ) as status, patch.object(instance_mod, "shell") as shell:
        r = runner.invoke(cli, ["instance", "shell", instance.name], obj=ctx)
    status.assert_called_once_with(ctx, instance)
    assert not shell.called
    assert r.exit_code == 1
    assert "instance is not_running" in r.stdout

    with patch.object(
        instance_mod, "status", return_value=instance_mod.Status.running
    ) as status, patch.object(instance_mod, "shell") as shell:
        runner.invoke(cli, ["instance", "shell", instance.name, "-U", "bob"], obj=ctx)
    status.assert_called_once_with(ctx, instance)
    shell.assert_called_once_with(ctx, instance, user="bob", dbname=None)


def test_instance_backup(runner, instance, ctx):
    patch_backup = patch.object(pgbackrest, "backup")
    patch_expire = patch.object(pgbackrest, "expire")
    with patch_backup as backup, patch_expire as expire:
        result = runner.invoke(
            cli,
            ["instance", "backup", instance.name, instance.version, "--type=diff"],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    assert backup.call_count == 1
    assert backup.call_args[1] == {"type": pgbackrest.BackupType("diff")}
    assert not expire.called

    with patch_backup as backup, patch_expire as expire:
        result = runner.invoke(
            cli,
            ["instance", "backup", instance.name, instance.version, "--purge"],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    assert backup.called
    assert expire.called


def test_instance_restore_list(runner, instance, ctx):
    bck = interface.InstanceBackup(
        label="foo",
        size=12,
        repo_size=13,
        datetime=datetime.datetime(2012, 1, 1),
        type="incr",
        databases="postgres, prod",
    )
    with patch.object(pgbackrest, "iter_backups", return_value=[bck]) as iter_backups:
        result = runner.invoke(
            cli,
            ["instance", "restore", instance.name, instance.version, "--list"],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    assert iter_backups.call_count == 1

    assert result.stdout == (
        "label      size    repo_size  datetime             type    databases\n"
        "-------  ------  -----------  -------------------  ------  --------------\n"
        "foo          12           13  2012-01-01 00:00:00  incr    postgres, prod\n"
    )


def test_instance_restore(runner, instance, ctx):
    with patch("pglift.instance.status", return_value=Status.running) as status:
        result = runner.invoke(
            cli,
            ["instance", "restore", instance.name, instance.version],
            obj=ctx,
        )
    assert result.exit_code == 1, result
    assert "instance is running" in result.stdout
    status.assert_called_once_with(ctx, instance)

    with patch.object(pgbackrest, "restore") as restore:
        result = runner.invoke(
            cli,
            [
                "instance",
                "restore",
                instance.name,
                instance.version,
                "--label=xyz",
            ],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    assert restore.called_once_with(ctx, instance, label="xyz")


def test_instance_privileges(ctx, instance, runner, running):
    with patch(
        "pglift.privileges.get",
        return_value=[
            interface.Privilege(
                database="db2",
                schema="public",
                role="rol2",
                object_type="FUNCTION",
                privileges=["EXECUTE"],
            ),
        ],
    ) as privileges_get:
        result = runner.invoke(
            cli,
            [
                "instance",
                "privileges",
                instance.name,
                instance.version,
                "--json",
                "-d",
                "db2",
                "-r",
                "rol2",
            ],
            obj=ctx,
        )
    assert result.exit_code == 0, result.stdout
    privileges_get.assert_called_once_with(
        ctx, instance, databases=("db2",), roles=("rol2",)
    )
    assert json.loads(result.stdout) == [
        {
            "database": "db2",
            "schema": "public",
            "role": "rol2",
            "object_type": "FUNCTION",
            "privileges": ["EXECUTE"],
        }
    ]


def test_role_create(ctx, instance, runner, running):
    with patch.object(roles, "exists", return_value=False) as exists, patch.object(
        roles, "apply"
    ) as apply:
        result = runner.invoke(
            cli,
            [
                "role",
                "create",
                f"{instance.version}/{instance.name}",
                "rob",
                "--password=ert",
                "--pgpass",
                "--no-inherit",
                "--in-role=monitoring",
                "--in-role=backup",
            ],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    exists.assert_called_once_with(ctx, instance, "rob")
    role = interface.Role.parse_obj(
        {
            "name": "rob",
            "password": "ert",
            "pgpass": True,
            "inherit": False,
            "in_roles": ["monitoring", "backup"],
        }
    )
    apply.assert_called_once_with(ctx, instance, role)
    running.assert_called_once_with(ctx, instance)

    running.reset_mock()

    with patch.object(roles, "exists", return_value=True) as exists:
        result = runner.invoke(
            cli,
            [
                "role",
                "create",
                f"{instance.version}/{instance.name}",
                "bob",
            ],
            obj=ctx,
        )
    assert result.exit_code == 1
    assert "role already exists" in result.stdout
    exists.assert_called_once_with(ctx, instance, "bob")
    running.assert_called_once_with(ctx, instance)


def test_role_alter(runner, ctx, instance, running):
    actual = interface.Role(name="alterme", connection_limit=3)
    altered = interface.Role(
        name="alterme",
        connection_limit=30,
        pgpass=True,
        password="blah",
        login=True,
        inherit=False,
    )

    with patch.object(roles, "describe", return_value=actual) as describe, patch.object(
        roles, "apply"
    ) as apply:
        result = runner.invoke(
            cli,
            [
                "role",
                "alter",
                str(instance),
                "alterme",
                "--connection-limit=30",
                "--pgpass",
                "--password=blah",
                "--login",
                "--no-inherit",
            ],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "alterme")
    apply.assert_called_once_with(ctx, instance, altered)
    assert result.exit_code == 0, result.output


def test_role_schema(runner):
    result = runner.invoke(cli, ["role", "schema"])
    schema = json.loads(result.output)
    assert schema["title"] == "Role"
    assert schema["description"] == "PostgreSQL role"


def test_role_apply(runner, tmp_path, ctx, instance, running):
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
    running.assert_called_once_with(ctx, instance)
    (call_ctx, call_instance, call_role), kwargs = apply.call_args
    assert call_ctx == ctx
    assert call_instance == instance
    assert call_role.name == "roltest"
    assert kwargs == {}


def test_role_describe(runner, ctx, instance, running):
    with patch.object(
        roles, "describe", side_effect=exceptions.RoleNotFound("absent")
    ) as describe:
        result = runner.invoke(
            cli,
            ["role", "describe", str(instance), "absent"],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "absent")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1, (result, result.output)
    assert result.stdout.strip() == "Error: role 'absent' not found"

    running.reset_mock()

    with patch.object(
        roles,
        "describe",
        return_value=interface.Role.parse_obj(
            {
                "name": "present",
                "pgpass": True,
                "password": "hidden",
                "inherit": False,
                "validity": datetime.datetime(2022, 1, 1),
                "connection_limit": 5,
                "in_roles": ["observers", "monitoring"],
            }
        ),
    ) as describe:
        result = runner.invoke(
            cli,
            ["role", "describe", instance.name, "present"],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "present")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0
    described = yaml.safe_load(result.stdout)
    assert described == {
        "name": "present",
        "password": "**********",
        "pgpass": True,
        "inherit": False,
        "login": False,
        "connection_limit": 5,
        "validity": "2022-01-01T00:00:00",
        "in_roles": ["observers", "monitoring"],
    }


def test_role_drop(runner, ctx, instance, running):
    with patch.object(
        roles, "drop", side_effect=exceptions.RoleNotFound("bar")
    ) as drop:
        result = runner.invoke(
            cli,
            ["role", "drop", str(instance), "foo"],
            obj=ctx,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1
    assert result.stdout.strip() == "Error: role 'bar' not found"

    running.reset_mock()

    with patch.object(roles, "drop") as drop:
        result = runner.invoke(
            cli,
            ["role", "drop", str(instance), "foo"],
            obj=ctx,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0


def test_role_privileges(ctx, instance, runner, running):
    with patch(
        "pglift.privileges.get",
        return_value=[
            interface.Privilege(
                database="db2",
                schema="public",
                role="rol2",
                object_type="FUNCTION",
                privileges=["EXECUTE"],
            ),
        ],
    ) as privileges_get, patch.object(roles, "describe") as role_describe:
        result = runner.invoke(
            cli,
            [
                "role",
                "privileges",
                str(instance),
                "rol2",
                "--json",
                "-d",
                "db2",
            ],
            obj=ctx,
        )
    assert result.exit_code == 0, result.stdout
    privileges_get.assert_called_once_with(
        ctx, instance, databases=("db2",), roles=("rol2",)
    )
    role_describe.assert_called_once_with(ctx, instance, "rol2")
    assert json.loads(result.stdout) == [
        {
            "database": "db2",
            "schema": "public",
            "role": "rol2",
            "object_type": "FUNCTION",
            "privileges": ["EXECUTE"],
        }
    ]


def test_database_create(ctx, instance, runner, running):
    with patch.object(databases, "exists", return_value=False) as exists, patch.object(
        databases, "apply"
    ) as apply:
        result = runner.invoke(
            cli,
            [
                "database",
                "create",
                f"{instance.version}/{instance.name}",
                "db_test1",
            ],
            obj=ctx,
        )
    assert result.exit_code == 0, result
    exists.assert_called_once_with(ctx, instance, "db_test1")
    database = interface.Database.parse_obj({"name": "db_test1"})
    apply.assert_called_once_with(ctx, instance, database)
    running.assert_called_once_with(ctx, instance)

    running.reset_mock()

    with patch.object(databases, "exists", return_value=True) as exists:
        result = runner.invoke(
            cli,
            [
                "database",
                "create",
                f"{instance.version}/{instance.name}",
                "db_test2",
            ],
            obj=ctx,
        )
    assert result.exit_code == 1
    assert "database already exists" in result.stdout
    exists.assert_called_once_with(ctx, instance, "db_test2")
    running.assert_called_once_with(ctx, instance)


def test_database_alter(runner, ctx, instance, running):
    actual = interface.Database(name="alterme")
    altered = interface.Database(name="alterme", owner="dba")

    with patch.object(
        databases, "describe", return_value=actual
    ) as describe, patch.object(databases, "apply") as apply:
        result = runner.invoke(
            cli,
            [
                "database",
                "alter",
                str(instance),
                "alterme",
                "--owner=dba",
            ],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "alterme")
    apply.assert_called_once_with(ctx, instance, altered)
    assert result.exit_code == 0, result.output


def test_database_schema(runner):
    result = runner.invoke(cli, ["database", "schema"])
    schema = json.loads(result.output)
    assert schema["title"] == "Database"
    assert schema["description"] == "PostgreSQL database"


def test_database_apply(runner, tmp_path, ctx, instance, running):
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump({"name": "dbtest"})
    manifest.write_text(content)
    with patch.object(databases, "apply") as apply:
        result = runner.invoke(
            cli,
            ["database", "apply", str(instance), "-f", str(manifest)],
            obj=ctx,
        )
    assert result.exit_code == 0
    apply.assert_called_once()
    running.assert_called_once_with(ctx, instance)
    (call_ctx, call_instance, call_database), kwargs = apply.call_args
    assert call_ctx == ctx
    assert call_instance == instance
    assert call_database.name == "dbtest"
    assert kwargs == {}


def test_database_describe(runner, ctx, instance, running):
    with patch.object(
        databases, "describe", side_effect=exceptions.DatabaseNotFound("absent")
    ) as describe:
        result = runner.invoke(
            cli,
            ["database", "describe", str(instance), "absent"],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "absent")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1
    assert result.stdout.strip() == "Error: database 'absent' not found"

    running.reset_mock()

    with patch.object(
        databases,
        "describe",
        return_value=interface.Database(name="present", owner="dba"),
    ) as describe:
        result = runner.invoke(
            cli,
            ["database", "describe", instance.name, "present"],
            obj=ctx,
        )
    describe.assert_called_once_with(ctx, instance, "present")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0
    described = yaml.safe_load(result.stdout)
    assert described == {"name": "present", "owner": "dba"}


def test_database_list(runner, ctx, instance, running):
    with patch.object(
        databases,
        "list",
        return_value=[
            interface.DetailedDatabase(
                name="template1",
                owner="postgres",
                encoding="UTF8",
                collation="C",
                ctype="C",
                acls=["=c/postgres", "postgres=CTc/postgres"],
                size=8167939,
                description="default template for new databases",
                tablespace=interface.Tablespace(
                    name="pg_default", location="", size=41011771
                ),
            )
        ],
    ) as list_:
        result = runner.invoke(
            cli,
            ["database", "list", instance.name, "--json"],
            obj=ctx,
        )
    list_.assert_called_once_with(ctx, instance)
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0, result.stdout
    dbs = json.loads(result.stdout)
    assert dbs == [
        {
            "acls": ["=c/postgres", "postgres=CTc/postgres"],
            "collation": "C",
            "ctype": "C",
            "description": "default template for new databases",
            "encoding": "UTF8",
            "name": "template1",
            "owner": "postgres",
            "size": 8167939,
            "tablespace": {"location": "", "name": "pg_default", "size": 41011771},
        }
    ]


def test_database_drop(runner, ctx, instance, running):
    with patch.object(
        databases, "drop", side_effect=exceptions.DatabaseNotFound("bar")
    ) as drop:
        result = runner.invoke(
            cli,
            ["database", "drop", str(instance), "foo"],
            obj=ctx,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1
    assert result.stdout.strip() == "Error: database 'bar' not found"

    running.reset_mock()

    with patch.object(databases, "drop") as drop:
        result = runner.invoke(
            cli,
            ["database", "drop", str(instance), "foo"],
            obj=ctx,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0


def test_database_privileges(ctx, instance, runner, running):
    with patch(
        "pglift.privileges.get",
        return_value=[
            interface.Privilege(
                database="db2",
                schema="public",
                role="rol2",
                object_type="FUNCTION",
                privileges=["EXECUTE"],
            ),
        ],
    ) as privileges_get, patch.object(databases, "describe") as databases_describe:
        result = runner.invoke(
            cli,
            [
                "database",
                "privileges",
                str(instance),
                "db2",
                "--json",
                "-r",
                "rol2",
            ],
            obj=ctx,
        )
    assert result.exit_code == 0, result.stdout
    privileges_get.assert_called_once_with(
        ctx, instance, databases=("db2",), roles=("rol2",)
    )
    databases_describe.assert_called_once_with(ctx, instance, "db2")
    assert json.loads(result.stdout) == [
        {
            "database": "db2",
            "schema": "public",
            "role": "rol2",
            "object_type": "FUNCTION",
            "privileges": ["EXECUTE"],
        }
    ]


@pytest.mark.parametrize("action", ["start", "stop"])
def test_postgres_exporter(runner, ctx, instance, action):
    with patch.object(prometheus, action) as patched:
        result = runner.invoke(
            cli,
            ["postgres_exporter", action, str(instance)],
            obj=ctx,
        )
    patched.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0, result
