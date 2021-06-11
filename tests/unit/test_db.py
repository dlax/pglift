import json

from pglift import db


def test_queries(datadir, regen_test_data):
    actual = dict(db.queries())
    fpath = datadir / "queries.json"
    if regen_test_data:
        with fpath.open("w") as f:
            json.dump(actual, f, indent=2, sort_keys=True)
    expected = json.loads(fpath.read_text())
    assert actual == expected


def test_query():
    query = db.query("role_alter_password", username="bob")
    qs = "".join(q.string for q in query.seq)
    assert qs == "ALTER ROLE bob PASSWORD %(password)s"
