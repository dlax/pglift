import datetime
import functools
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple, Type
from unittest.mock import MagicMock, patch

import click
import psycopg
import pytest
import yaml
from click.shell_completion import ShellComplete
from click.testing import CliRunner
from pgtoolkit.ctl import Status

from pglift import _install, databases, exceptions, instances, prometheus, roles, types
from pglift.cli import CLIContext, Obj, cli
from pglift.cli import instance as instance_cli
from pglift.cli.util import Command, get_instance, pass_component_settings, pass_ctx
from pglift.ctx import Context
from pglift.models import interface, system
from pglift.models.system import Instance
from pglift.pgbackrest import impl as pgbackrest
from pglift.pgbackrest.cli import pgbackrest as pgbackrest_cli
from pglift.prometheus import impl as prometheus_impl
from pglift.prometheus.cli import postgres_exporter as postgres_exporter_cli
from pglift.settings import PostgreSQLVersion, Settings
from pglift.temboard.cli import temboard_agent as temboard_agent_cli

instance_arg_guessed_or_given = pytest.mark.parametrize(
    "args", [[], ["test"]], ids=["instance:guessed", "instance:given"]
)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


@pytest.fixture
def ctx(settings: Settings) -> CLIContext:
    return CLIContext(settings=settings)


@pytest.fixture
def obj(ctx: CLIContext) -> Obj:
    return Obj(context=ctx)


@pytest.fixture
def running(ctx: Context, instance: Instance) -> Iterator[MagicMock]:
    with patch("pglift.instances.running") as m:
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


@pytest.mark.parametrize(
    "logpath_exists", [False, True], ids=lambda v: f"logpath_exists:{v}"
)
def test_command_error(runner: CliRunner, obj: Obj, logpath_exists: bool) -> None:
    logpath = obj.ctx.settings.logpath
    if logpath_exists:
        logpath.mkdir()
    result = runner.invoke(cmd, ["error"], obj=obj)
    assert result.exit_code == 1
    assert (
        result.stderr
        == "Error: Command '['bad', 'cmd']' returned non-zero exit status 1.\nerrs\noutput\n"
    )
    assert not list(logpath.glob("*.log"))
    if logpath_exists:
        assert logpath.exists()
    else:
        assert not logpath.exists()


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


def test_instance_identifier(runner: CliRunner, obj: Obj, instance: Instance) -> None:
    @click.command(cls=Command)
    @instance_cli.instance_identifier(nargs=1)
    def one(instance: system.Instance) -> None:
        """One"""
        click.echo(instance, nl=False)

    @click.command(cls=Command)
    @instance_cli.instance_identifier(nargs=-1)
    def many(instance: Tuple[system.Instance]) -> None:
        """Many"""
        click.echo(", ".join(str(i) for i in instance), nl=False)

    result = runner.invoke(one, [], obj=obj)
    assert result.exit_code == 0, result.stderr
    assert result.stdout == str(instance)

    result = runner.invoke(many, [], obj=obj)
    assert result.exit_code == 0, result.stderr
    assert result.stdout == str(instance)

    result = runner.invoke(one, [str(instance)], obj=obj)
    assert result.exit_code == 0, result.stderr
    assert result.stdout == str(instance)

    result = runner.invoke(many, [str(instance), instance.name], obj=obj)
    assert result.exit_code == 0, result.stderr
    assert result.stdout == f"{instance}, {instance}"


def test_instance_commands_completion(runner: CliRunner, obj: Obj) -> None:
    group = instance_cli.cli
    assert group.name
    comp = ShellComplete(group, {"obj": obj}, group.name, "_CLICK_COMPLETE")
    commands = [c.value for c in comp.get_completions([], "")]
    assert commands == [
        "alter",
        "backup",
        "backups",
        "create",
        "drop",
        "env",
        "exec",
        "get",
        "list",
        "logs",
        "privileges",
        "promote",
        "reload",
        "restart",
        "restore",
        "start",
        "status",
        "stop",
        "upgrade",
    ]


def test_obj(monkeypatch: pytest.MonkeyPatch) -> None:
    with monkeypatch.context() as m:
        m.setenv("SETTINGS", json.dumps({"invalid": None}))
        with pytest.raises(click.ClickException, match="invalid site settings"):
            Obj()


def test_cli(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cli, obj=obj)
    assert result.exit_code == 0


def test_non_interactive(runner: CliRunner) -> None:
    @cli.command("confirmme")
    @pass_ctx
    def confirm_me(ctx: Context) -> None:
        if not ctx.confirm("Confirm?", default=True):
            raise click.Abort()
        print("confirmed")

    result = runner.invoke(cli, ["confirmme"], input="n\n")
    assert result.exit_code == 1 and "Aborted!" in result.stderr

    result = runner.invoke(cli, ["--non-interactive", "confirmme"])
    assert result.exit_code == 0 and "confirmed" in result.stdout


def test_version(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cli, ["--version"], obj=obj)
    assert re.match(r"pglift version (\d\.).*", result.stdout)


def test_site_settings(runner: CliRunner, ctx: Context, obj: Obj) -> None:
    result = runner.invoke(cli, ["site-settings"], obj=obj)
    assert result.exit_code == 0, result.stderr
    settings = json.loads(result.output)
    assert settings == json.loads(ctx.settings.json())

    result = runner.invoke(cli, ["site-settings", "--schema"], obj=obj)
    assert result.exit_code == 0, result.stderr
    schema = json.loads(result.output)
    schema.pop("title")
    expected = json.loads(ctx.settings.schema_json())
    expected.pop("title")
    assert schema == expected


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


@pytest.mark.parametrize("shell", ["bash", "fish", "zsh"])
def test_completion(runner: CliRunner, shell: str) -> None:
    result = runner.invoke(cli, ["completion", shell])
    assert result.exit_code == 0, result
    assert "_pglift_completion" in result.output


def test_instance_create(
    runner: CliRunner,
    ctx: Context,
    obj: Obj,
    instance: Instance,
    composite_instance_model: Type[interface.Instance],
    pg_version: str,
) -> None:
    with patch.object(instances, "apply") as apply:
        result = runner.invoke(
            cli,
            ["instance", "create", instance.name, f"--version={instance.version}"],
            obj=obj,
        )
    assert not apply.call_count
    assert result.exit_code == 1
    assert "instance already exists" in result.stderr

    cmd = [
        "instance",
        "create",
        "new",
        f"--version={pg_version}",
        "--port=1234",
        "--locale=fr_FR.UTF8",
        "--encoding=LATIN1",
        "--data-checksums",
        "--extension=unaccent",
        "--extension=pg_stat_statements",
        "--auth-host=ident",
        "--prometheus-port=1212",
        "--temboard-port=2347",
    ]
    with patch.object(instances, "apply") as apply:
        result = runner.invoke(cli, cmd, obj=obj)
    expected = {
        "name": "new",
        "version": pg_version,
        "port": 1234,
        "locale": "fr_FR.UTF8",
        "encoding": "LATIN1",
        "data_checksums": True,
        "extensions": ["unaccent", "pg_stat_statements"],
        "auth": {
            "local": None,
            "host": "ident",
        },
        "prometheus": {"port": 1212},
        "temboard": {"port": 2347},
    }
    e = composite_instance_model.parse_obj(expected)
    apply.assert_called_once_with(ctx, e)
    assert result.exit_code == 0, result


def test_instance_create_standby(
    runner: CliRunner,
    ctx: Context,
    obj: Obj,
    composite_instance_model: Type[interface.Instance],
) -> None:
    cmd = [
        "instance",
        "create",
        "stdby",
        "--standby-for=port=1234 user=repli",
        "--standby-slot=sloot",
        "--standby-password=replicated",
    ]
    with patch.object(instances, "apply") as apply:
        result = runner.invoke(cli, cmd, obj=obj)
    expected = {
        "name": "stdby",
        "standby": {
            "for": "port=1234 user=repli",
            "slot": "sloot",
            "password": "replicated",
        },
        "prometheus": {"port": 9187},
        "temboard": {"port": 2345},
    }

    e = composite_instance_model.parse_obj(expected)
    apply.assert_called_once_with(ctx, e)
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

    m = {
        "name": "test",
        "prometheus": {"password": "truite", "port": 1212},
        "temboard": {"port": 2347},
    }
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump(m)
    manifest.write_text(content)
    with patch.object(instances, "apply") as apply:
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

    actual_obj: Dict[str, Any] = {
        "name": instance.name,
        "extensions": ["pg_stat_statements", "unaccent"],
        "prometheus": {"port": 1212},
        "temboard": {"port": 2347},
    }
    altered_obj: Dict[str, Any] = {
        "name": instance.name,
        "state": "stopped",
        "extensions": ["passwordcheck"],
        "prometheus": {"port": 2121},
        "temboard": {"port": 2437},
    }
    cmd = [
        "instance",
        "alter",
        str(instance),
        "--state=stopped",
        "--extension=passwordcheck",
        "--prometheus-port=2121",
        "--temboard-port=2437",
    ]
    actual = composite_instance_model.parse_obj(actual_obj)
    altered = composite_instance_model.parse_obj(altered_obj)
    with patch.object(instances, "apply") as apply, patch.object(
        instances, "_get", return_value=actual
    ) as _get:
        result = runner.invoke(cli, cmd, obj=obj)
    _get.assert_called_once_with(ctx, instance)
    apply.assert_called_once_with(ctx, altered)
    assert result.exit_code == 0, result.output


def test_instance_promote(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    result = runner.invoke(cli, ["instance", "promote", "notfound"], obj=obj)
    assert result.exit_code == 2, result.stderr
    assert "instance 'notfound' not found" in result.stderr
    with patch.object(instances, "promote") as promote:
        result = runner.invoke(cli, ["instance", "promote", str(instance)], obj=obj)
    assert result.exit_code == 0, result.stderr
    promote.assert_called_once_with(ctx, instance)


def test_instance_schema(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(cli, ["instance", "--schema"], obj=obj)
    schema = json.loads(result.output)
    assert schema["title"] == "Instance"
    assert schema["description"] == "PostgreSQL instance"


@instance_arg_guessed_or_given
def test_instance_get(
    runner: CliRunner,
    ctx: Context,
    obj: Obj,
    instance: Instance,
    pg_version: str,
    args: List[str],
) -> None:
    manifest = interface.Instance.parse_obj(
        {
            "name": "test",
            "locale": "C",
            "encoding": "UTF16",
            "surole_password": "ahaha",
            "standby": {"for": "host=primary"},
        }
    )
    with patch.object(instances, "get", return_value=manifest) as get:
        json_result = runner.invoke(cli, ["instance", "get", "--json"] + args, obj=obj)
    get.assert_called_once_with(ctx, "test", pg_version)
    assert json_result.exit_code == 0, (json_result, json_result.output)
    assert '"name": "test"' in json_result.output

    with patch.object(instances, "get", return_value=manifest) as get:
        table_result = runner.invoke(cli, ["instance", "get"] + args, obj=obj)
    get.assert_called_once_with(ctx, "test", pg_version)
    assert table_result.exit_code == 0, (table_result, table_result.output)
    assert table_result.output.splitlines() == [
        " name  versi…  port  ssl    surol…  replr…  data…  locale  enco…  exten…  pend… ",
        " test          5432  False  *****…                 C       UTF16          False ",
    ]


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

    ver = next(iter(PostgreSQLVersion))
    with patch.object(instances, "list") as list_instances:
        result = runner.invoke(
            cli, ["instance", "list", f"--version={ver.value}"], obj=obj
        )
    list_instances.assert_called_once_with(ctx, version=ver)


@instance_arg_guessed_or_given
def test_instance_drop(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, args: List[str]
) -> None:
    with patch.object(instances, "drop") as patched:
        result = runner.invoke(cli, ["instance", "drop"] + args, obj=obj)
    assert result.exit_code == 0, (result, result.output)
    patched.assert_called_once_with(ctx, instance)


def test_instance_status(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj
) -> None:
    with patch.object(instances, "status", return_value=Status.not_running) as patched:
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
    with patch.object(instances, action) as patched:
        result = runner.invoke(cli, ["instance", action, str(instance)], obj=obj)
    assert result.exit_code == 0, result
    patched.assert_called_once_with(ctx, instance, **kwargs)


def test_instance_exec(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj
) -> None:
    with patch.object(instances, "exec") as instance_exec:
        r = runner.invoke(
            cli,
            ["instance", "exec", instance.name],
            obj=obj,
        )
    assert not instance_exec.called
    assert r.exit_code == 1
    assert r.stderr == "Error: no command given\n"

    with patch.object(instances, "exec") as instance_exec:
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
    bindir = instances.pg_ctl(instance.version, ctx=ctx).bindir
    path = os.environ["PATH"]
    expected = "\n".join(
        [
            f"PATH={bindir}:{path}",
            f"PGBACKREST_CONFIG={ctx.settings.prefix}/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf",
            f"PGBACKREST_STANZA={instance.version}-{instance.name}",
            f"PGDATA={instance.datadir}",
            "PGHOST=/socks",
            f"PGPASSFILE={ctx.settings.postgresql.auth.passfile}",
            "PGPORT=999",
            "PGUSER=postgres",
            f"PSQLRC={instance.psqlrc}",
            f"PSQL_HISTORY={instance.psql_history}",
        ]
    )
    assert r.stdout.rstrip() == expected


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


def test_instance_backup(runner: CliRunner, instance: Instance, obj: Obj) -> None:
    with patch.object(pgbackrest, "backup") as backup:
        result = runner.invoke(
            cli,
            ["instance", "backup", str(instance), "--type=diff"],
            obj=obj,
        )
    assert result.exit_code == 0, result
    assert backup.call_count == 1
    assert backup.call_args[1] == {"type": types.BackupType("diff")}


def test_instance_backups(
    runner: CliRunner, instance: Instance, settings: Settings, ctx: Context, obj: Obj
) -> None:
    bck = interface.InstanceBackup(
        label="foo",
        size=12,
        repo_size=13,
        date_start=datetime.datetime(2012, 1, 1),
        date_stop=datetime.datetime(2012, 1, 2),
        type="incr",
        databases=["postgres", "prod"],
    )
    with patch.object(pgbackrest, "iter_backups", return_value=[bck]) as iter_backups:
        result = runner.invoke(
            cli,
            ["instance", "backups", str(instance)],
            obj=obj,
        )
    assert result.exit_code == 0, result
    assert iter_backups.call_count == 1

    assert [
        v.strip() for v in result.stdout.splitlines()[-3].split("│") if v.strip()
    ] == [
        "foo",
        "12.0B",
        "13.0B",
        "2012-01-01",
        "2012-01-02",
        "incr",
        "postgres,",
    ]

    assert [
        v.strip() for v in result.stdout.splitlines()[-2].split("│") if v.strip()
    ] == [
        "00:00:00",
        "00:00:00",
        "prod",
    ]

    with patch.object(pgbackrest, "iter_backups", return_value=[bck]) as iter_backups:
        result = runner.invoke(
            cli,
            ["instance", "backups", str(instance), "--json"],
            obj=obj,
        )
    assert result.exit_code == 0, result
    iter_backups.assert_called_once_with(ctx, instance, settings.pgbackrest)
    assert json.loads(result.stdout) == [
        {
            "databases": ["postgres", "prod"],
            "date_start": "2012-01-01T00:00:00",
            "date_stop": "2012-01-02T00:00:00",
            "label": "foo",
            "repo_size": 13,
            "size": 12,
            "type": "incr",
        }
    ]


def test_instance_restore(
    runner: CliRunner, instance: Instance, ctx: Context, obj: Obj
) -> None:
    with patch("pglift.instances.status", return_value=Status.running) as status:
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
            interface.DefaultPrivilege(
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
                "--default",
            ],
            obj=obj,
        )
    assert result.exit_code == 0, result.stdout
    privileges_get.assert_called_once_with(
        ctx, instance, databases=("db2",), roles=("rol2",), defaults=True
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
    newversion = next(iter(PostgreSQLVersion))
    with patch.object(
        instances, "upgrade", return_value=new_instance
    ) as upgrade, patch.object(instances, "start") as start:
        result = runner.invoke(
            cli,
            [
                "instance",
                "upgrade",
                str(instance),
                "--name=new",
                "--port=12",
                "--jobs=3",
                f"--version={newversion.value}",
            ],
            obj=obj,
        )
    assert result.exit_code == 0, result.stdout
    upgrade.assert_called_once_with(
        ctx, instance, version=newversion, name="new", port=12, jobs=3
    )
    start.assert_called_once_with(ctx, new_instance)


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
    result = runner.invoke(
        cli, ["pgconf", "-i", str(instance), "show"] + params, obj=obj
    )
    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "\n".join(expected)

    result = runner.invoke(
        cli, ["pgconf", "-i", str(instance), "show", "port"], obj=obj
    )
    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "\n".join(["port = 999"])


def test_pgconf_set_validate(runner: CliRunner, obj: Obj, instance: Instance) -> None:
    result = runner.invoke(
        cli,
        ["pgconf", "-i", str(instance), "set", "invalid"],
        obj=obj,
    )
    assert result.exit_code == 2
    assert "Error: Invalid value for '<PARAMETER>=<VALUE>...': invalid" in result.stderr


def test_pgconf_set(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    with patch.object(
        instances, "configure", return_value={"foo": ("baz", "bar")}
    ) as configure:
        result = runner.invoke(
            cli,
            [
                "pgconf",
                "-i",
                str(instance),
                "set",
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
        instances,
        "configure",
        return_value={"bonjour_name": ("test", "changed")},
    ) as configure:
        result = runner.invoke(
            cli,
            [
                "pgconf",
                "-i",
                str(instance),
                "set",
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
        ["pgconf", "-i", str(instance), "remove", "fsync"],
        obj=obj,
    )
    assert result.exit_code == 1
    assert "'fsync' not found in managed configuration" in result.stderr

    with patch.object(
        instances, "configure", return_value={"bonjour_name": ("test", None)}
    ) as configure:
        result = runner.invoke(
            cli,
            ["pgconf", f"--instance={instance}", "remove", "bonjour_name"],
            obj=obj,
        )
    manifest = interface.Instance(name=instance.name, version=instance.version)
    configure.assert_called_once_with(ctx, manifest, values={"bonjour": True})
    assert result.exit_code == 0, result.stderr
    assert "bonjour_name: test -> None" in result.stderr


def test_pgconf_edit(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    user_conf = instance.datadir / "conf.pglift.d" / "user.conf"
    with patch("click.edit", return_value="bonjour = bonsoir\n") as edit, patch.object(
        instances, "configure", return_value={"bonjour": ("on", "'matin")}
    ) as configure:
        result = runner.invoke(
            cli,
            ["pgconf", f"--instance={instance}", "edit"],
            obj=obj,
        )
    assert result.exit_code == 0, result.stderr
    edit.assert_called_once_with(text=user_conf.read_text())
    manifest = interface.Instance(name=instance.name, version=instance.version)
    configure.assert_called_once_with(ctx, manifest, values={"bonjour": "bonsoir"})
    assert result.stderr == "bonjour: on -> 'matin\n"

    with patch("click.edit", return_value=None) as edit, patch.object(
        instances, "configure"
    ) as configure:
        result = runner.invoke(
            cli, ["pgconf", f"--instance={instance}", "edit"], obj=obj
        )
    assert not configure.called
    assert result.stderr == "no change\n"


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
                f"--instance={instance.version}/{instance.name}",
                "create",
                "rob",
                "--password=ert",
                "--pgpass",
                "--login",
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
            "login": True,
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
                f"--instance={instance.version}/{instance.name}",
                "create",
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
    actual = interface.Role(name="alterme", connection_limit=3, in_roles=["pg_monitor"])
    altered = interface.Role(
        name="alterme",
        connection_limit=30,
        pgpass=True,
        password="blah",
        login=True,
        inherit=False,
        in_roles=["pg_read_all_data", "pg_read_all_settings"],
    )

    with patch.object(roles, "get", return_value=actual) as get, patch.object(
        roles, "apply"
    ) as apply:
        result = runner.invoke(
            cli,
            [
                "role",
                "-i",
                str(instance),
                "alter",
                "alterme",
                "--connection-limit=30",
                "--pgpass",
                "--password=blah",
                "--login",
                "--no-inherit",
                "--in-role=pg_read_all_data",
                "--in-role=pg_read_all_settings",
            ],
            obj=obj,
        )
    get.assert_called_once_with(ctx, instance, "alterme")
    apply.assert_called_once_with(ctx, instance, altered)
    assert result.exit_code == 0, result.output


def test_role_schema(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["role", "--schema"])
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
    m = {"name": "roltest", "pgpass": True}
    content = yaml.dump(m)
    manifest.write_text(content)
    with patch.object(roles, "apply") as apply:
        result = runner.invoke(
            cli,
            ["role", "-i", str(instance), "apply", "-f", str(manifest)],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once_with(ctx, instance, interface.Role.parse_obj(m))
    running.assert_called_once_with(ctx, instance)


def test_role_get(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(
        roles, "get", side_effect=exceptions.RoleNotFound("absent")
    ) as get:
        result = runner.invoke(
            cli,
            ["role", "-i", str(instance), "get", "absent"],
            obj=obj,
        )
    get.assert_called_once_with(ctx, instance, "absent")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1, (result, result.output)
    assert result.stderr.strip() == "Error: role 'absent' not found"

    running.reset_mock()

    with patch.object(
        roles,
        "get",
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
    ) as get:
        result = runner.invoke(
            cli,
            ["role", "-i", instance.name, "get", "present", "--json"],
            obj=obj,
        )
    get.assert_called_once_with(ctx, instance, "present")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
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
            ["role", f"--instance={instance}", "drop", "foo"],
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
            ["role", "-i", str(instance), "drop", "foo"],
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
            interface.DefaultPrivilege(
                database="db2",
                schema="public",
                role="rol2",
                object_type="FUNCTION",
                privileges=["EXECUTE"],
            ),
        ],
    ) as privileges_get, patch.object(roles, "get") as roles_get:
        result = runner.invoke(
            cli,
            [
                "role",
                "-i",
                str(instance),
                "privileges",
                "rol2",
                "--json",
                "-d",
                "db2",
                "--default",
            ],
            obj=obj,
        )
    assert result.exit_code == 0, result.stdout
    privileges_get.assert_called_once_with(
        ctx, instance, databases=("db2",), roles=("rol2",), defaults=True
    )
    roles_get.assert_called_once_with(ctx, instance, "rol2")
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
                f"--instance={instance.version}/{instance.name}",
                "create",
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
                f"--instance={instance.version}/{instance.name}",
                "create",
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

    with patch.object(databases, "get", return_value=actual) as get, patch.object(
        databases, "apply"
    ) as apply:
        result = runner.invoke(
            cli,
            [
                "database",
                f"--instance={instance}",
                "alter",
                "alterme",
                "--owner=dba",
            ],
            obj=obj,
        )
    get.assert_called_once_with(ctx, instance, "alterme")
    apply.assert_called_once_with(ctx, instance, altered)
    assert result.exit_code == 0, result.output


def test_database_schema(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["database", "--schema"])
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
    m = {"name": "dbtest"}
    content = yaml.dump(m)
    manifest.write_text(content)
    with patch.object(databases, "apply") as apply:
        result = runner.invoke(
            cli,
            ["database", "-i", str(instance), "apply", "-f", str(manifest)],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once_with(ctx, instance, interface.Database.parse_obj(m))
    running.assert_called_once_with(ctx, instance)


def test_database_get(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(
        databases, "get", side_effect=exceptions.DatabaseNotFound("absent")
    ) as get:
        result = runner.invoke(
            cli,
            ["database", "-i", str(instance), "get", "absent"],
            obj=obj,
        )
    get.assert_called_once_with(ctx, instance, "absent")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 1
    assert result.stderr.strip() == "Error: database 'absent' not found"

    running.reset_mock()

    with patch.object(
        databases,
        "get",
        return_value=interface.Database(name="present", owner="dba"),
    ) as get:
        result = runner.invoke(
            cli,
            ["database", "-i", instance.name, "get", "present", "--json"],
            obj=obj,
        )
    get.assert_called_once_with(ctx, instance, "present")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "extensions": [],
        "name": "present",
        "owner": "dba",
        "settings": None,
    }


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
            ["database", "-i", instance.name, "list", "template1", "--json"],
            obj=obj,
        )
    list_.assert_called_once_with(ctx, instance, dbnames=("template1",))
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
            ["database", "-i", str(instance), "drop", "foo"],
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
            ["database", "-i", str(instance), "drop", "foo"],
            obj=obj,
        )
    drop.assert_called_once_with(ctx, instance, "foo")
    running.assert_called_once_with(ctx, instance)
    assert result.exit_code == 0


def test_database_run(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(databases, "run", return_value={"db": [{"name": "bob"}]}) as run:
        result = runner.invoke(
            cli,
            [
                "database",
                "-i",
                str(instance),
                "run",
                "--json",
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
    dbs = json.loads(result.stdout)
    assert dbs == {"db": [{"name": "bob"}]}


def test_database_run_programmingerror(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance, running: MagicMock
) -> None:
    with patch.object(
        databases, "run", side_effect=psycopg.ProgrammingError("bingo")
    ) as run:
        result = runner.invoke(
            cli,
            ["database", "-i", str(instance), "run", "some sql"],
            obj=obj,
        )
    run.assert_called_once_with(
        ctx, instance, "some sql", dbnames=(), exclude_dbnames=()
    )
    assert result.exit_code == 1
    assert result.stderr == "Error: bingo\n"


def test_database_privileges(
    ctx: Context, obj: Obj, instance: Instance, runner: CliRunner, running: MagicMock
) -> None:
    with patch(
        "pglift.privileges.get",
        return_value=[
            interface.DefaultPrivilege(
                database="db2",
                schema="public",
                role="rol2",
                object_type="FUNCTION",
                privileges=["EXECUTE"],
            ),
        ],
    ) as privileges_get, patch.object(databases, "get") as databases_get:
        result = runner.invoke(
            cli,
            [
                "database",
                "-i",
                str(instance),
                "privileges",
                "db2",
                "--json",
                "-r",
                "rol2",
                "--default",
            ],
            obj=obj,
        )
    assert result.exit_code == 0, result.stdout
    privileges_get.assert_called_once_with(
        ctx, instance, databases=("db2",), roles=("rol2",), defaults=True
    )
    databases_get.assert_called_once_with(ctx, instance, "db2")
    assert json.loads(result.stdout) == [
        {
            "database": "db2",
            "schema": "public",
            "role": "rol2",
            "object_type": "FUNCTION",
            "privileges": ["EXECUTE"],
        }
    ]


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
    with patch.object(prometheus_impl, action) as patched:
        result = runner.invoke(
            postgres_exporter_cli,
            [action, instance.qualname],
            obj=obj,
        )
    assert result.exit_code == 0, result.stderr
    patched.assert_called_once_with(
        ctx, instance.qualname, ctx.settings.prometheus, **kwargs
    )


def test_postgres_exporter_schema(runner: CliRunner, obj: Obj) -> None:
    result = runner.invoke(postgres_exporter_cli, ["--schema"], obj=obj)
    schema = json.loads(result.output)
    assert schema["title"] == "PostgresExporter"
    assert schema["description"] == "Prometheus postgres_exporter service."


def test_postgres_exporter_apply(
    runner: CliRunner, tmp_path: Path, ctx: Context, obj: Obj
) -> None:
    manifest = tmp_path / "manifest.yml"
    content = yaml.dump({"name": "123-exp", "dsn": "dbname=monitoring", "port": 123})
    manifest.write_text(content)
    with patch.object(prometheus_impl, "apply") as apply:
        result = runner.invoke(
            postgres_exporter_cli,
            ["apply", "-f", str(manifest)],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once_with(
        ctx,
        prometheus.PostgresExporter(name="123-exp", dsn="dbname=monitoring", port=123),
        ctx.settings.prometheus,
    )


def test_postgres_exporter_install(runner: CliRunner, ctx: Context, obj: Obj) -> None:
    with patch.object(prometheus_impl, "apply") as apply:
        result = runner.invoke(
            postgres_exporter_cli,
            ["install", "123-exp", "dbname=monitoring", "123"],
            obj=obj,
        )
    assert result.exit_code == 0
    apply.assert_called_once_with(
        ctx,
        prometheus.PostgresExporter(name="123-exp", dsn="dbname=monitoring", port=123),
        ctx.settings.prometheus,
    )


def test_postgres_exporter_uninstall(runner: CliRunner, ctx: Context, obj: Obj) -> None:
    with patch.object(prometheus_impl, "drop") as drop:
        result = runner.invoke(
            postgres_exporter_cli,
            ["uninstall", "123-exp"],
            obj=obj,
        )
    assert result.exit_code == 0
    drop.assert_called_once_with(ctx, "123-exp")


def test_pgbackrest(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    with patch.object(ctx, "run") as run:
        result = runner.invoke(
            pgbackrest_cli, ["-i", str(instance), "info", "--output=json"], obj=obj
        )
    assert result.exit_code == 0, result.stderr
    prefix = ctx.settings.prefix
    stanza = f"{instance.version}-{instance.name}"
    run.assert_called_once_with(
        [
            "/usr/bin/pgbackrest",
            f"--config={prefix}/etc/pgbackrest/pgbackrest-{stanza}.conf",
            f"--stanza={stanza}",
            "info",
            "--output=json",
        ],
        redirect_output=True,
        check=True,
    )


def test_temboard_secret_key(
    runner: CliRunner, ctx: Context, obj: Obj, instance: Instance
) -> None:
    result = runner.invoke(temboard_agent_cli, "secret-key", obj=obj)
    assert result.exit_code == 0
    assert json.loads(result.output) == 123456789
