import datetime
import functools
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Type
from unittest.mock import MagicMock, patch

import click
import pytest
import yaml
from click.testing import CliRunner
from pgtoolkit.ctl import Status

from pglift import _install, databases, exceptions
from pglift import instance as instance_mod
from pglift import pgbackrest, prometheus, roles
from pglift.cli import (
    CLIContext,
    Command,
    Obj,
    cli,
    get_instance,
    pass_component_settings,
)
from pglift.ctx import Context
from pglift.models import interface
from pglift.models.system import Instance
from pglift.settings import Settings


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


@pytest.fixture
def ctx(settings: Settings) -> CLIContext:
    return CLIContext(settings=settings)


@pytest.fixture
def obj(ctx: CLIContext) -> Obj:
    return Obj(ctx, None)


@pytest.fixture
def running(ctx: Context, instance: Instance) -> Iterator[MagicMock]:
    with patch("pglift.instance.running") as m:
        yield m
    m.assert_called_once_with(ctx, instance)


@click.command(cls=Command)
@click.argument("error")
@click.pass_context
def cmd(ctx: click.Context, error: str) -> None:
    if error == "error":
        raise exceptions.CommandError(1, ["bad", "cmd"], "output", "errs")
    if error == "cancel":
        raise exceptions.Cancelled("flop")
    if error == "runtimeerror":
        raise RuntimeError("oups")
    if error == "exit":
        ctx.exit(1)


def test_command_error(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cmd, ["error"], obj=obj)
    assert result.exit_code == 1
    assert (
        result.stderr
        == "Error: Command '['bad', 'cmd']' returned non-zero exit status 1.\nerrs\noutput\n"
    )
    logpath = obj.ctx.settings.logpath
    assert not list(logpath.glob("*.log"))


def test_command_cancelled(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cmd, ["cancel"], obj=obj)
    assert result.exit_code == 1
    assert result.stderr == "Aborted!\n"
    logpath = obj.ctx.settings.logpath
    assert not list(logpath.glob("*.log"))


def test_command_exit(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cmd, ["exit"], obj=obj)
    assert result.exit_code == 1
    assert not result.stdout
    logpath = obj.ctx.settings.logpath
    assert not list(logpath.glob("*.log"))


def test_command_internal_error(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cmd, ["runtimeerror"], obj=obj)
    assert result.exit_code == 1
    logpath = obj.ctx.settings.logpath
    logfile = next(logpath.glob("*.log"))
    logcontent = logfile.read_text()
    assert "an unexpected error occurred" in logcontent
    assert "Traceback (most recent call last):" in logcontent
    assert "RuntimeError: oups" in logcontent


def test_pass_component_settings(runner: CliRunner, obj: Obj) -> None:
    mod = MagicMock()

    @click.command("command")
    @functools.partial(pass_component_settings, mod, "mymod")
    def command(settings: Any, *args: Any) -> None:
        click.echo(f"settings is {settings.id}")

    mod.available.return_value = None
    r = runner.invoke(command, obj=obj)
    assert r.exit_code == 1
    assert r.stderr == "mymod not available\n"

    rv = MagicMock(id="123")
    mod.available.return_value = rv
    r = runner.invoke(command, obj=obj)
    assert r.exit_code == 0
    assert r.stdout == "settings is 123\n"


def test_get_instance(ctx: Context, instance: Instance) -> None:
    assert get_instance(ctx, instance.name, instance.version) == instance

    assert get_instance(ctx, instance.name, None) == instance

    with pytest.raises(click.BadParameter):
        get_instance(ctx, "notfound", None)

    with pytest.raises(click.BadParameter):
        get_instance(ctx, "notfound", instance.version)

    with patch.object(Instance, "system_lookup") as system_lookup:
        with pytest.raises(
            click.BadParameter,
            match="instance 'foo' exists in several PostgreSQL version",
        ):
            get_instance(ctx, "foo", None)
    assert system_lookup.call_count == 2


def test_cli(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cli, obj=obj)
    assert result.exit_code == 0


def test_version(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cli, ["--version"], obj=obj)
    assert re.match(r"pglift version (\d\.).*", result.stdout)


def test_site_settings(runner: CliRunner, ctx: Context, obj: Obj) -> None:
    result = runner.invoke(cli, ["site-settings"], obj=obj)
    assert result.exit_code == 0, result.stderr
    settings = json.loads(result.output)
    assert settings == json.loads(ctx.settings.json())


def test_site_configure(
    runner: CliRunner, ctx: Context, obj: Obj, tmp_path: Path
) -> None:
    with patch.object(_install, "do") as do_install:
        result = runner.invoke(
            cli, ["site-configure", "install", f"--settings={tmp_path}"], obj=obj
        )
    assert result.exit_code == 0, result
    do_install.assert_called_once_with(ctx, env=f"SETTINGS=@{tmp_path}")

    with patch.object(_install, "undo") as undo_install:
        result = runner.invoke(cli, ["site-configure", "uninstall"], obj=obj)
    assert result.exit_code == 0, result
    undo_install.assert_called_once_with(ctx)


def test_instance_create(
    runner: CliRunner,
    ctx: Context,
    obj: Obj,
    instance: Instance,
    composite_instance_model: Type[interface.Instance],
) -> None:
    with patch.object(instance_mod, "apply") as apply:
        result = runner.invoke(
            cli,
            ["instance", "create", instance.name, f"--version={instance.version}"],
            obj=obj,
        )
    assert not apply.call_count
    assert result.exit_code == 1
    assert "instance already exists" in result.stderr

    cmd = ["instance", "create", "new", "--port=1234", "--data-checksums"]
    if ctx.settings.prometheus is not None:
        cmd.append("--prometheus-port=1212")
    with patch.object(instance_mod, "apply") as apply:
        result = runner.invoke(cli, cmd, obj=obj)
    expected = {"name": "new", "port": 1234, "data_checksums": True}
    if ctx.settings.prometheus is not None:
        expected["prometheus"] = {"port": 1212}
    apply.assert_called_once_with(ctx, composite_instance_model.parse_obj(expected))
    assert result.exit_code == 0, result


def test_instance_apply(
    tmp_path: Path,
    runner: CliRunner,
    ctx: Context,
    obj: Obj,
    composite_instance_model: Type[interface.Instance],
) -> None:
    result = runner.invoke(cli, ["--log-level=debug", "instance", "apply"], obj=obj)
    assert result.exit_code == 2
    assert "Missing option '-f'" in result.stderr

    m: Dict[str, Any] = {"name": "test"}
    if ctx.settings.prometheus is not None:
        m["prometheus"] = {"port": 1212}
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump(m)
    manifest.write_text(content)
    mock_instance = object()
    with patch.object(
        instance_mod, "apply", return_value=(mock_instance, {}, False)
    ) as apply:
        result = runner.invoke(cli, ["instance", "apply", "-f", str(manifest)], obj=obj)
    assert result.exit_code == 0, (result, result.output)
    apply.assert_called_once_with(ctx, composite_instance_model.parse_obj(m))


def test_instance_alter(
    runner: CliRunner,
    ctx: Context,
    obj: Obj,
    instance: Instance,
    composite_instance_model: Type[interface.Instance],
) -> None:
    result = runner.invoke(
        cli, ["instance", "alter", "11/notfound", "--port=1"], obj=obj
    )
    assert result.exit_code == 2, result.stderr
    assert "instance '11/notfound' not found" in result.stderr

    actual_obj: Dict[str, Any] = {"name": instance.name}
    altered_obj: Dict[str, Any] = {"name": instance.name, "state": "stopped"}
    cmd = ["instance", "alter", str(instance), "--state=stopped"]
    if ctx.settings.prometheus is not None:
        actual_obj["prometheus"] = {"port": 1212}
        altered_obj["prometheus"] = {"port": 2121}
        cmd.append("--prometheus-port=2121")
    actual = composite_instance_model.parse_obj(actual_obj)
    altered = composite_instance_model.parse_obj(altered_obj)
    with patch.object(
        instance_mod, "apply", return_value=(instance, {}, True)
    ) as apply, patch.object(instance_mod, "describe", return_value=actual) as describe:
        result = runner.invoke(cli, cmd, obj=obj)
    describe.assert_called_once_with(ctx, instance.name, instance.version)
    apply.assert_called_once_with(ctx, altered)
    assert result.exit_code == 0, result.output


def test_instance_promote(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    result = runner.invoke(cli, ["instance", "promote", "notfound"], obj=obj)
    assert result.exit_code == 2, result.stderr
    assert "instance 'notfound' not found" in result.stderr
    with patch.object(instance_mod, "promote") as promote:
        result = runner.invoke(cli, ["instance", "promote", str(instance)], obj=obj)
    assert result.exit_code == 0, result.stderr
    promote.assert_called_once_with(ctx, instance)


def test_instance_schema(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cli, ["instance", "schema"], obj=obj)
    schema = json.loads(result.output)
    assert schema["title"] == "Instance"
    assert schema["description"] == "PostgreSQL instance"


def test_instance_describe(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, pg_version: str
) -> None:
    result = runner.invoke(cli, ["instance", "describe"], obj=obj)
    assert result.exit_code == 2
    assert "Missing argument '<version>/<name>'" in result.stderr

    manifest = interface.Instance(name="test")
    with patch.object(instance_mod, "describe", return_value=manifest) as describe:
        result = runner.invoke(cli, ["instance", "describe", "test"], obj=obj)
    assert result.exit_code == 0, (result, result.output)
    describe.assert_called_once_with(ctx, "test", pg_version)
    assert "name: test" in result.output


def test_instance_list(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj, tmp_path: Path
) -> None:
    name, version = instance.name, instance.version
    port = instance.config().port
    path = instance.path
    expected_list_as_json = [
        {
            "name": name,
            "path": str(path),
            "port": port,
            "status": "not_running",
            "version": version,
        }
    ]
    logfile = tmp_path / "logfile"
    result = runner.invoke(
        cli,
        ["--log-level=debug", f"--log-file={logfile}", "instance", "list", "--json"],
        obj=obj,
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == expected_list_as_json

    assert "pg_ctl --version" in logfile.read_text()

    result = runner.invoke(
        cli, ["instance", "list", "--json", f"--version={instance.version}"], obj=obj
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == expected_list_as_json

    other_version = next(
        v for v in ctx.settings.postgresql.versions if v != instance.version
    )
    result = runner.invoke(
        cli, ["instance", "list", "--json", f"--version={other_version}"], obj=obj
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == []
    result = runner.invoke(
        cli, ["instance", "list", f"--version={other_version}"], obj=obj
    )
    assert result.exit_code == 0
    assert not result.output


@pytest.mark.parametrize(
    "params, expected",
    [
        ([], ["port = 999", "unix_socket_directories = '/socks'"]),
        (["port"], ["port = 999"]),
        (["backslash_quote"], ["# backslash_quote = 'safe_encoding'"]),
    ],
    ids=["param=<none>", "param=port", "param=backslash_quote(commented)"],
)
def test_pgconf_show(
    runner: CliRunner,
    obj: Obj,
    instance: Instance,
    params: List[str],
    expected: List[str],
) -> None:
    result = runner.invoke(cli, ["pgconf", "show", str(instance)] + params, obj=obj)
    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "\n".join(expected)

    result = runner.invoke(cli, ["pgconf", "show", str(instance), "port"], obj=obj)
    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "\n".join(["port = 999"])


def test_pgconf_set_validate(runner: CliRunner, obj: Obj, instance: Instance) -> None:
    result = runner.invoke(
        cli,
        ["pgconf", "set", str(instance), "invalid"],
        obj=obj,
    )
    assert result.exit_code == 2
    assert "Error: Invalid value for '<PARAMETER>=<VALUE>...': invalid" in result.stderr


def test_pgconf_set(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    with patch.object(
        instance_mod, "configure", return_value={"foo": ("baz", "bar")}
    ) as configure:
        result = runner.invoke(
            cli,
            [
                "pgconf",
                "set",
                str(instance),
                "cluster_name=unittests",
                "foo=bar",
            ],
            obj=obj,
        )
    assert result.exit_code == 0
    manifest = interface.Instance(name=instance.name, version=instance.version)
    configure.assert_called_once_with(
        ctx,
        manifest,
        values=dict(
            bonjour=True,
            bonjour_name="test",
            cluster_name="unittests",
            foo="bar",
        ),
    )
    assert "foo: baz -> bar" in result.stderr

    with patch.object(
        instance_mod, "configure", return_value={"bonjour_name": ("test", "changed")}
    ) as configure:
        result = runner.invoke(
            cli,
            [
                "pgconf",
                "set",
                str(instance),
                "foo=bar",
                "bonjour_name=changed",
            ],
            obj=obj,
        )
    assert result.exit_code == 0
    manifest = interface.Instance(name=instance.name, version=instance.version)
    configure.assert_called_once_with(
        ctx,
        manifest,
        values=dict(
            bonjour=True,
            bonjour_name="changed",
            foo="bar",
        ),
    )
    assert "bonjour_name: test -> changed" in result.stderr
    assert "foo: baz -> bar" not in result.stderr
    assert "changes in 'foo' not applied" in result.stderr
    assert "\n hint:" in result.stderr


def test_pgconf_remove(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    result = runner.invoke(
        cli,
        ["pgconf", "remove", str(instance), "fsync"],
        obj=obj,
    )
    assert result.exit_code == 1
    assert "'fsync' not found in managed configuration" in result.stderr

    with patch.object(
        instance_mod, "configure", return_value={"bonjour_name": ("test", None)}
    ) as configure:
        result = runner.invoke(
            cli,
            ["pgconf", "remove", str(instance), "bonjour_name"],
            obj=obj,
        )
    manifest = interface.Instance(name=instance.name, version=instance.version)
    configure.assert_called_once_with(ctx, manifest, values={"bonjour": True})
    assert result.exit_code == 0, result.stderr
    assert "bonjour_name: test -> None" in result.stderr


def test_pgconf_edit(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    with patch("click.edit") as edit:
        result = runner.invoke(
            cli,
            ["pgconf", "edit", str(instance)],
            obj=obj,
        )
    assert result.exit_code == 0, result.stderr
    edit.assert_called_once_with(
        filename=str(instance.datadir / "conf.pglift.d" / "user.conf")
    )


def test_instance_drop(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    result = runner.invoke(cli, ["instance", "drop"], obj=obj)
    assert result.exit_code == 2
    assert "Missing argument '<version>/<name>'" in result.stderr

    with patch.object(instance_mod, "drop") as patched:
        result = runner.invoke(cli, ["instance", "drop", "test"], obj=obj)
    assert result.exit_code == 0, (result, result.output)
    patched.assert_called_once_with(ctx, instance)


def test_instance_status(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj
) -> None:
    with patch.object(
        instance_mod, "status", return_value=Status.not_running
    ) as patched:
        result = runner.invoke(cli, ["instance", "status", instance.name], obj=obj)
    assert result.exit_code == 3, (result, result.output)
    assert result.stdout == "not running\n"
    patched.assert_called_once_with(ctx, instance)


@pytest.mark.parametrize(
    ["action", "kwargs"],
    [("start", {"foreground": False}), ("stop", {}), ("reload", {}), ("restart", {})],
)
def test_instance_operations(
    runner: CliRunner,
    instance: Instance,
    ctx: Context,
    obj: Obj,
    action: str,
    kwargs: Dict[str, bool],
) -> None:
    with patch.object(instance_mod, action) as patched:
        result = runner.invoke(cli, ["instance", action, str(instance)], obj=obj)
    assert result.exit_code == 0, result
    patched.assert_called_once_with(ctx, instance, **kwargs)


def test_instance_exec(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj
) -> None:
    with patch.object(instance_mod, "exec") as instance_exec:
        r = runner.invoke(
            cli,
            ["instance", "exec", instance.name],
            obj=obj,
        )
    assert not instance_exec.called
    assert r.exit_code == 1
    assert r.stderr == "Error: no command given\n"

    with patch.object(instance_mod, "exec") as instance_exec:
        runner.invoke(
            cli,
            ["instance", "exec", instance.name, "--", "psql", "-d", "test"],
            obj=obj,
        )
    instance_exec.assert_called_once_with(ctx, instance, ("psql", "-d", "test"))


def test_instance_env(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj
) -> None:
    r = runner.invoke(
        cli,
        ["instance", "env", instance.name],
        obj=obj,
    )
    assert r.exit_code == 0, r
    bindir = ctx.pg_ctl(instance.version).bindir
    path = os.environ["PATH"]
    assert r.stdout == (
        f"PATH={bindir}:{path}\n"
        f"PGDATA={instance.datadir}\n"
        "PGHOST=/socks\n"
        f"PGPASSFILE={ctx.settings.postgresql.auth.passfile}\n"
        "PGPORT=999\n"
        "PGUSER=postgres\n"
    )


def test_instance_logs(runner: CliRunner, instance: Instance, obj: Obj) -> None:
    result = runner.invoke(cli, ["instance", "logs", str(instance)], obj=obj)
    assert result.exit_code == 1, result
    assert (
        result.stderr
        == f"Error: file 'current_logfiles' for instance {instance} not found\n"
    )

    stderr_logpath = instance.datadir / "log" / "postgresql.log"
    stderr_logpath.parent.mkdir()
    stderr_logpath.write_text("log\nged\n")
    (instance.datadir / "current_logfiles").write_text(f"stderr {stderr_logpath}\n")
    result = runner.invoke(cli, ["instance", "logs", str(instance)], obj=obj)
    assert result.exit_code == 0, result.stderr
    assert result.output == "log\nged\n"


@pytest.mark.usefixtures("need_pgbackrest")
def test_instance_backup(runner: CliRunner, instance: Instance, obj: Obj) -> None:
    with patch.object(pgbackrest, "backup") as backup:
        result = runner.invoke(
            cli,
            ["instance", "backup", str(instance), "--type=diff"],
            obj=obj,
        )
    assert result.exit_code == 0, result
    assert backup.call_count == 1
    assert backup.call_args[1] == {"type": pgbackrest.BackupType("diff")}


@pytest.mark.usefixtures("need_pgbackrest")
def test_instance_restore_list(runner: CliRunner, instance: Instance, obj: Obj) -> None:
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
            ["instance", "restore", str(instance), "--list"],
            obj=obj,
        )
    assert result.exit_code == 0, result
    assert iter_backups.call_count == 1

    assert [
        v.strip() for v in result.stdout.splitlines()[-2].split("│") if v.strip()
    ] == [
        "foo",
        "12.0",
        "13.0",
        "2012-01-01 00:00:00",
        "incr",
        "postgres, prod",
    ]


@pytest.mark.usefixtures("need_pgbackrest")
def test_instance_restore(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj
) -> None:
    with patch("pglift.instance.status", return_value=Status.running) as status:
        result = runner.invoke(
            cli,
            ["instance", "restore", str(instance)],
            obj=obj,
        )
    assert result.exit_code == 1, result
    assert "instance is running" in result.stderr
    status.assert_called_once_with(ctx, instance)

    with patch.object(pgbackrest, "restore") as restore:
        result = runner.invoke(
            cli,
            ["instance", "restore", str(instance), "--label=xyz"],
            obj=obj,
        )
    assert result.exit_code == 0, result
    assert restore.called_once_with(ctx, instance, label="xyz")


def test_instance_privileges(
    ctx: Context, obj: Obj, instance: Instance, runner: CliRunner, running: MagicMock
) -> None:
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
                str(instance),
                "--json",
                "-d",
                "db2",
                "-r",
                "rol2",
            ],
            obj=obj,
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


def test_instance_upgrade(
    ctx: Context, obj: Obj, instance: Instance, runner: CliRunner
) -> None:
    new_instance = MagicMock()
    with patch.object(
        instance_mod, "upgrade", return_value=new_instance
    ) as upgrade, patch.object(instance_mod, "start") as start:
        result = runner.invoke(
            cli,
            [
                "instance",
                "upgrade",
                str(instance),
                "--name=new",
                "--port=12",
                "--jobs=3",
            ],
            obj=obj,
        )
    assert result.exit_code == 0, result.stdout
    upgrade.assert_called_once_with(
        ctx, instance, version=None, name="new", port=12, jobs=3
    )
    start.assert_called_once_with(ctx, new_instance)


def test_role_create(
    ctx: Context, obj: Obj, instance: Instance, runner: CliRunner, running: MagicMock
) -> None:
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
            obj=obj,
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
            obj=obj,
        )
    assert result.exit_code == 1
    assert "role already exists" in result.stderr
    exists.assert_called_once_with(ctx, instance, "bob")
    running.assert_called_once_with(ctx, instance)


def test_role_alter(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
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
            obj=obj,
        )
    describe.assert_called_once_with(ctx, instance, "alterme")
    apply.assert_called_once_with(ctx, instance, altered)
    assert result.exit_code == 0, result.output


def test_role_schema(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["role", "schema"])
    schema = json.loads(result.output)
    assert schema["title"] == "Role"
    assert schema["description"] == "PostgreSQL role"


def test_role_apply(
    runner: CliRunner,
    tmp_path: Path,
    ctx: Context,
    obj: Obj,
    instance: Instance,
    running: MagicMock,
) -> None:
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump({"name": "roltest", "pgpass": True})
    manifest.write_text(content)
    with patch.object(roles, "apply") as apply:
        result = runner.invoke(
            cli,
            ["role", "apply", str(instance), "-f", str(manifest)],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once()
    running.assert_called_once_with(ctx, instance)
    (call_ctx, call_instance, call_role), kwargs = apply.call_args
    assert call_ctx == ctx
    assert call_instance == instance
    assert call_role.name == "roltest"
    assert kwargs == {}


def test_role_describe(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(
        roles, "describe", side_effect=exceptions.RoleNotFound("absent")
    ) as describe:
        result = runner.invoke(
            cli,
            ["role", "describe", str(instance), "absent"],
            obj=obj,
        )
    describe.assert_called_once_with(ctx, instance, "absent")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1, (result, result.output)
    assert result.stderr.strip() == "Error: role 'absent' not found"

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
            obj=obj,
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
        "superuser": False,
        "replication": False,
        "connection_limit": 5,
        "validity": "2022-01-01T00:00:00",
        "in_roles": ["observers", "monitoring"],
    }


def test_role_drop(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(
        roles, "drop", side_effect=exceptions.RoleNotFound("bar")
    ) as drop:
        result = runner.invoke(
            cli,
            ["role", "drop", str(instance), "foo"],
            obj=obj,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1
    assert result.stderr.splitlines()[-1] == "Error: role 'bar' not found"

    running.reset_mock()

    with patch.object(roles, "drop") as drop:
        result = runner.invoke(
            cli,
            ["role", "drop", str(instance), "foo"],
            obj=obj,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0


def test_role_privileges(
    ctx: Context, obj: Obj, instance: Instance, runner: CliRunner, running: MagicMock
) -> None:
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
            obj=obj,
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


def test_database_create(
    ctx: Context, obj: Obj, instance: Instance, runner: CliRunner, running: MagicMock
) -> None:
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
            obj=obj,
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
            obj=obj,
        )
    assert result.exit_code == 1
    assert "database already exists" in result.stderr
    exists.assert_called_once_with(ctx, instance, "db_test2")
    running.assert_called_once_with(ctx, instance)


def test_database_alter(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
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
            obj=obj,
        )
    describe.assert_called_once_with(ctx, instance, "alterme")
    apply.assert_called_once_with(ctx, instance, altered)
    assert result.exit_code == 0, result.output


def test_database_schema(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["database", "schema"])
    schema = json.loads(result.output)
    assert schema["title"] == "Database"
    assert schema["description"] == "PostgreSQL database"


def test_database_apply(
    runner: CliRunner,
    tmp_path: Path,
    ctx: Context,
    obj: Obj,
    instance: Instance,
    running: MagicMock,
) -> None:
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump({"name": "dbtest"})
    manifest.write_text(content)
    with patch.object(databases, "apply") as apply:
        result = runner.invoke(
            cli,
            ["database", "apply", str(instance), "-f", str(manifest)],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once()
    running.assert_called_once_with(ctx, instance)
    (call_ctx, call_instance, call_database), kwargs = apply.call_args
    assert call_ctx == ctx
    assert call_instance == instance
    assert call_database.name == "dbtest"
    assert kwargs == {}


def test_database_describe(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(
        databases, "describe", side_effect=exceptions.DatabaseNotFound("absent")
    ) as describe:
        result = runner.invoke(
            cli,
            ["database", "describe", str(instance), "absent"],
            obj=obj,
        )
    describe.assert_called_once_with(ctx, instance, "absent")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1
    assert result.stderr.strip() == "Error: database 'absent' not found"

    running.reset_mock()

    with patch.object(
        databases,
        "describe",
        return_value=interface.Database(name="present", owner="dba"),
    ) as describe:
        result = runner.invoke(
            cli,
            ["database", "describe", instance.name, "present"],
            obj=obj,
        )
    describe.assert_called_once_with(ctx, instance, "present")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0
    described = yaml.safe_load(result.stdout)
    assert described == {"name": "present", "owner": "dba", "settings": None}


def test_database_list(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
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
            obj=obj,
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


def test_database_drop(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(
        databases, "drop", side_effect=exceptions.DatabaseNotFound("bar")
    ) as drop:
        result = runner.invoke(
            cli,
            ["database", "drop", str(instance), "foo"],
            obj=obj,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1
    assert result.stderr.splitlines()[-1] == "Error: database 'bar' not found"

    running.reset_mock()

    with patch.object(databases, "drop") as drop:
        result = runner.invoke(
            cli,
            ["database", "drop", str(instance), "foo"],
            obj=obj,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0


def test_database_run(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(databases, "run") as run:
        result = runner.invoke(
            cli,
            [
                "database",
                "run",
                str(instance),
                "-d",
                "db",
                "some sql",
            ],
            obj=obj,
        )
    run.assert_called_once_with(
        ctx, instance, "some sql", dbnames=("db",), exclude_dbnames=()
    )
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0, result.stderr


def test_database_privileges(
    ctx: Context, obj: Obj, instance: Instance, runner: CliRunner, running: MagicMock
) -> None:
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
            obj=obj,
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


@pytest.mark.usefixtures("need_prometheus")
@pytest.mark.parametrize(
    ("action", "kwargs"),
    [("start", {"foreground": False}), ("stop", {})],
)
def test_postgres_exporter_start_stop(
    runner: CliRunner,
    ctx: Context,
    obj: Obj,
    instance: Instance,
    action: str,
    kwargs: Dict[str, bool],
) -> None:
    with patch.object(prometheus, action) as patched:
        result = runner.invoke(
            cli,
            ["postgres_exporter", action, instance.qualname],
            obj=obj,
        )
    patched.assert_called_once_with(
        ctx, instance.qualname, ctx.settings.prometheus, **kwargs
    )
    assert result.exit_code == 0, result


@pytest.mark.usefixtures("need_prometheus")
def test_postgres_exporter_schema(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["postgres_exporter", "schema"])
    schema = json.loads(result.output)
    assert schema["title"] == "PostgresExporter"
    assert schema["description"] == "Prometheus postgres_exporter service."


@pytest.mark.usefixtures("need_prometheus")
def test_postgres_exporter_apply(
    runner: CliRunner, tmp_path: Path, ctx: Context, obj: Obj
) -> None:
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump({"name": "123-exp", "dsn": "dbname=monitoring", "port": 123})
    manifest.write_text(content)
    with patch.object(prometheus, "apply") as apply:
        result = runner.invoke(
            cli,
            ["postgres_exporter", "apply", "-f", str(manifest)],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once_with(
        ctx,
        prometheus.PostgresExporter(name="123-exp", dsn="dbname=monitoring", port=123),
        ctx.settings.prometheus,
    )


@pytest.mark.usefixtures("need_prometheus")
def test_postgres_exporter_install(runner: CliRunner, ctx: Context, obj: Obj) -> None:
    with patch.object(prometheus, "apply") as apply:
        result = runner.invoke(
            cli,
            ["postgres_exporter", "install", "123-exp", "dbname=monitoring", "123"],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once_with(
        ctx,
        prometheus.PostgresExporter(name="123-exp", dsn="dbname=monitoring", port=123),
        ctx.settings.prometheus,
    )


@pytest.mark.usefixtures("need_prometheus")
def test_postgres_exporter_uninstall(runner: CliRunner, ctx: Context, obj: Obj) -> None:
    with patch.object(prometheus, "drop") as drop:
        result = runner.invoke(
            cli,
            ["postgres_exporter", "uninstall", "123-exp"],
            obj=obj,
        )
    assert result.exit_code == 0
    drop.assert_called_once_with(ctx, "123-exp")


@pytest.mark.usefixtures("need_pgbackrest")
def test_pgbackrest(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    with patch.object(ctx, "run") as run:
        result = runner.invoke(cli, ["pgbackrest", str(instance), "help"], obj=obj)
    assert result.exit_code == 0, result.stderr
    prefix = ctx.settings.prefix
    stanza = f"{instance.version}-{instance.name}"
    run.assert_called_once_with(
        [
            "/usr/bin/pgbackrest",
            f"--config={prefix}/etc/pgbackrest/pgbackrest-{stanza}.conf",
            f"--stanza={stanza}",
            "help",
        ],
        redirect_output=True,
        check=True,
    )
