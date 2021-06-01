import json

from pglift import queries


def test_queries_iter(datadir, regen_test_data):
    actual = dict(queries.iter())
    fpath = datadir / "queries.json"
    if regen_test_data:
        with fpath.open("w") as f:
            json.dump(actual, f, indent=2, sort_keys=True)
    expected = json.loads(fpath.read_text())
    assert actual == expected


def test_queries_get():
    query = queries.get("role_alter_password", username="bob")
    qs = "".join(q.string for q in query.seq)
    assert qs == "ALTER ROLE bob PASSWORD %(password)s"
