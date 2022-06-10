import datetime
import json
import os
import pathlib
import secrets
import socket
import string
import subprocess
from typing import Callable, Dict, Iterator, Optional

import psycopg
import pytest
import yaml

from pglift import db

PLAYDIR = pathlib.Path(__file__).parent.parent.parent / "docs" / "ansible"


def generate_secret(length: int) -> str:
    return "".join(
        secrets.choice(string.ascii_letters + string.digits) for i in range(length)
    )


@pytest.fixture
def ansible_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["ANSIBLE_COLLECTIONS_PATH"] = str(
        pathlib.Path(__file__).parent.parent.parent / "ansible"
    )
    return env


@pytest.fixture
def call_playbook(
    tmp_path: pathlib.Path,
    ansible_env: Dict[str, str],
    pgbackrest_available: bool,
    prometheus_execpath: Optional[pathlib.Path],
    temboard_execpath: Optional[pathlib.Path],
) -> Iterator[Callable[[pathlib.Path], None]]:
    env = ansible_env.copy()
    env["ANSIBLE_VERBOSITY"] = "3"
    settings = {
        "prefix": str(tmp_path),
        "run_prefix": str(tmp_path / "run"),
        "postgresql": {
            "auth": {
                "local": "md5",
                "host": "md5",
                "passfile": str(tmp_path / "pgpass"),
            },
            "surole": {"pgpass": True},
            "root": str(tmp_path / "postgresql"),
        },
    }
    if pgbackrest_available:
        settings["pgbackrest"] = {}
    if prometheus_execpath:
        settings["prometheus"] = {"execpath": str(prometheus_execpath)}
    else:
        pytest.skip("prometheus not available")
    if temboard_execpath:
        settings["temboard"] = {"execpath": str(temboard_execpath)}
    else:
        pytest.skip("temboard not available")

    with (tmp_path / "config.json").open("w") as f:
        json.dump(settings, f)
    env["SETTINGS"] = f"@{tmp_path / 'config.json'}"

    with (tmp_path / "vault-pass").open("w") as f:
        f.write(generate_secret(32))
    env["ANSIBLE_VAULT_PASSWORD_FILE"] = str(tmp_path / "vault-pass")

    with (tmp_path / "vars").open("w") as f:
        passwords = {
            "postgresql_surole_password": "supers3kret",
            "prod_bob_password": "s3kret",
            "prometheus_role_password": "pr0m3th3u$",
            "temboard_role_password": "temb0@rd",
        }
        yaml.dump(passwords, f)
    subprocess.check_call(["ansible-vault", "encrypt", str(tmp_path / "vars")], env=env)

    def call(playfile: pathlib.Path) -> None:
        subprocess.check_call(
            [
                "ansible-playbook",
                "--extra-vars",
                f'@{tmp_path / "vars"}',
                str(playfile),
            ],
            env=env,
        )

    yield call
    call(PLAYDIR / "play3.yml")
    assert not (tmp_path / "pgpass").exists()


def cluster_name(dsn: str) -> str:
    with db.connect_dsn(dsn) as cnx:
        cur = cnx.execute("SELECT setting FROM pg_settings WHERE name = 'cluster_name'")
        row = cur.fetchone()
        assert row
        name = row["setting"]
        assert isinstance(name, str), name
        return name


@pytest.mark.parametrize(
    "module",
    ["instance", "dsn_info", "role", "database", "postgres_exporter"],
    ids=lambda v: f"module:{v}",
)
def test_doc(module: str, ansible_env: Dict[str, str]) -> None:
    subprocess.check_call(["ansible-doc", f"dalibo.pglift.{module}"], env=ansible_env)


def test_ansible(
    tmp_path: pathlib.Path, call_playbook: Callable[[pathlib.Path], None]
) -> None:
    call_playbook(PLAYDIR / "play1.yml")

    prod_dsn = "host=/tmp user=postgres password=supers3kret dbname=postgres port=5433"
    assert cluster_name(prod_dsn) == "prod"
    with db.connect_dsn(prod_dsn) as cnx:
        cnx.execute("SET TIME ZONE 'UTC'")
        cur = cnx.execute(
            "SELECT rolname,rolinherit,rolcanlogin,rolconnlimit,rolpassword,rolvaliduntil FROM pg_roles WHERE rolname = 'bob'"
        )
        assert cur.fetchone() == {
            "rolname": "bob",
            "rolinherit": True,
            "rolcanlogin": True,
            "rolconnlimit": 10,
            "rolpassword": "********",
            "rolvaliduntil": datetime.datetime(
                2025, 1, 1, tzinfo=datetime.timezone.utc
            ),
        }
        cur = cnx.execute(
            "SELECT r.rolname AS role, ARRAY_AGG(m.rolname) AS member_of FROM pg_auth_members JOIN pg_authid m ON pg_auth_members.roleid = m.oid JOIN pg_authid r ON pg_auth_members.member = r.oid GROUP BY r.rolname"
        )
        assert cur.fetchall() == [
            {"role": "bob", "member_of": ["pg_read_all_stats", "pg_signal_backend"]},
            {
                "role": "pg_monitor",
                "member_of": [
                    "pg_read_all_settings",
                    "pg_read_all_stats",
                    "pg_stat_scan_tables",
                ],
            },
            {"role": "prometheus", "member_of": ["pg_monitor"]},
        ]
        extensions = cnx.execute("SELECT extname FROM pg_extension").fetchall()

    installed = [r["extname"] for r in extensions]
    assert "pg_stat_statements" in installed
    assert "unaccent" in installed

    socket.create_connection(("localhost", 9186), 1)
    socket.create_connection(("localhost", 2344), 1)

    # test connection with bob to the db database
    with db.connect_dsn(
        "host=/tmp user=bob password=s3kret dbname=db port=5433"
    ) as cnx:
        row = cnx.execute("SHOW work_mem").fetchone()
        extensions = cnx.execute("SELECT extname FROM pg_extension").fetchall()
    assert row is not None
    assert row["work_mem"] == "3MB"
    installed = [r["extname"] for r in extensions]
    assert "unaccent" in installed

    # check preprod cluster & postgres_exporter
    preprod_dsn = "host=/tmp user=postgres password=supers3kret dbname=test port=5434"
    assert cluster_name(preprod_dsn) == "preprod"
    socket.create_connection(("localhost", 9188), 1)
    socket.create_connection(("localhost", 2346), 1)

    # check dev cluster, postgres_exporter and temboard-agent are stopped
    with pytest.raises(psycopg.OperationalError, match="No such file or directory"):
        cluster_name(
            "host=/tmp user=postgres password=supers3kret dbname=postgres port=5444"
        )
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("localhost", 9189), 1)
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("localhost", 2347), 1)

    call_playbook(PLAYDIR / "play2.yml")

    # prod running
    assert cluster_name(prod_dsn) == "prod"

    # pg_stat_statements extension is uninstalled
    with db.connect_dsn(prod_dsn) as cnx:
        extensions = cnx.execute("SELECT extname FROM pg_extension").fetchall()
    installed = [r["extname"] for r in extensions]
    assert "pg_stat_statements" not in installed
    assert "unaccent" in installed

    # bob user and db database no longer exists
    with pytest.raises(
        psycopg.OperationalError, match='password authentication failed for user "bob"'
    ):
        with db.connect_dsn(
            "host=/tmp user=bob password=s3kret dbname=template1 port=5433"
        ):
            pass
    with pytest.raises(psycopg.OperationalError, match='database "db" does not exist'):
        with db.connect_dsn(
            "host=/tmp user=postgres password=supers3kret dbname=db port=5433"
        ):
            pass

    # preprod stopped
    with pytest.raises(psycopg.OperationalError, match="No such file or directory"):
        assert cluster_name(preprod_dsn) == "preprod"
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("localhost", 9188), 1)
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("localhost", 2346), 1)

    # dev running
    dev_dsn = "host=/tmp user=postgres password=supers3kret dbname=postgres port=5455"
    assert cluster_name(dev_dsn) == "dev"
    socket.create_connection(("localhost", 9189))
    socket.create_connection(("localhost", 2347))
