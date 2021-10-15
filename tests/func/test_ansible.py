import datetime
import json
import os
import pathlib
import socket
import subprocess

import dateutil.tz
import psycopg2
import pytest

from pglift import db

PLAYDIR = pathlib.Path(__file__).parent.parent.parent / "docs" / "ansible"


@pytest.fixture
def call_playbook(tmpdir):
    env = os.environ.copy()
    env["ANSIBLE_COLLECTIONS_PATH"] = str(
        pathlib.Path(__file__).parent.parent.parent / "ansible"
    )
    env["ANSIBLE_VERBOSITY"] = "3"
    settings = {
        "prefix": str(tmpdir),
        "postgresql": {
            "auth": {"local": "md5", "passfile": str(tmpdir / "pgpass")},
            "surole": {"pgpass": True},
            "root": str(tmpdir / "postgresql"),
        },
    }
    with (tmpdir / "config.json").open("w") as f:
        json.dump(settings, f)
    env["SETTINGS"] = f"@{tmpdir / 'config.json'}"
    env["PGPASSFILE"] = str(tmpdir / "pgpass")
    env["postgresql_surole_password"] = "s3kret"

    def call(playfile: pathlib.Path) -> None:
        subprocess.check_call(["ansible-playbook", str(playfile)], env=env)

    os.environ["PGPASSFILE"] = env["PGPASSFILE"]
    yield call
    del os.environ["PGPASSFILE"]
    call(PLAYDIR / "play3.yml")
    assert not (tmpdir / "pgpass").exists()


def cluster_name(dsn: str) -> str:
    with db.connect_dsn(dsn) as cnx:
        with cnx.cursor() as cur:
            cur.execute("SELECT setting FROM pg_settings WHERE name = 'cluster_name'")
            name = cur.fetchall()[0][0]
            assert isinstance(name, str), name
            return name


def test_ansible(tmpdir, call_playbook):
    call_playbook(PLAYDIR / "play1.yml")
    assert "*:5433:*:bob:s3kret" in (tmpdir / "pgpass").read().splitlines()

    prod_dsn = "host=/tmp user=postgres dbname=postgres port=5433"
    assert cluster_name(prod_dsn) == "prod"
    with db.connect_dsn(prod_dsn) as cnx:
        with cnx.cursor() as cur:
            cur.execute(
                "SELECT rolname,rolinherit,rolcanlogin,rolconnlimit,rolpassword,rolvaliduntil FROM pg_roles WHERE rolname = 'bob'"
            )
            assert cur.fetchall() == [
                [
                    "bob",
                    True,
                    True,
                    10,
                    "********",
                    datetime.datetime(2025, 1, 1, tzinfo=dateutil.tz.tzlocal()),
                ]
            ]
            cur.execute(
                "SELECT r.rolname AS role, ARRAY_AGG(m.rolname) AS member_of FROM pg_auth_members JOIN pg_authid m ON pg_auth_members.roleid = m.oid JOIN pg_authid r ON pg_auth_members.member = r.oid GROUP BY r.rolname"
            )
            assert cur.fetchall() == [
                ["bob", ["pg_read_all_stats", "pg_signal_backend"]],
                [
                    "pg_monitor",
                    [
                        "pg_read_all_settings",
                        "pg_read_all_stats",
                        "pg_stat_scan_tables",
                    ],
                ],
            ]

    socket.create_connection(("localhost", 9186), 1)

    # check preprod cluster & postgres_exporter
    preprod_dsn = "host=/tmp user=postgres dbname=postgres port=5434"
    assert cluster_name(preprod_dsn) == "preprod"
    socket.create_connection(("localhost", 9188), 1)

    # check dev cluster & postgres_exporter are stopped
    with pytest.raises(psycopg2.OperationalError):
        cluster_name("host=/tmp user=postgres dbname=postgres port=5444")

    # check dev postgres_exporter is stopped
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("localhost", 9189), 1)

    call_playbook(PLAYDIR / "play2.yml")
    assert "bob" not in (tmpdir / "pgpass").read()

    # prod running
    assert cluster_name(prod_dsn) == "prod"

    # preprod stopped
    with pytest.raises(psycopg2.OperationalError):
        assert cluster_name(preprod_dsn) == "preprod"
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("localhost", 9188), 1)

    # dev running
    dev_dsn = "host=/tmp user=postgres dbname=postgres port=5455"
    assert cluster_name(dev_dsn) == "dev"
    socket.create_connection(("localhost", 9189))
