from pglift import pgpass


def test_add(tmp_path):
    passfile = tmp_path / "pgpass"
    pgpass.add(passfile, "ah")
    assert passfile.read_text().splitlines() == ["*:*:*:*:ah"]
    pgpass.add(passfile, "oh", username="bob", database="prod", port=1234)
    assert passfile.read_text().splitlines() == ["*:1234:prod:bob:oh", "*:*:*:*:ah"]
    pgpass.add(password="eh", passfile=passfile)
    assert passfile.read_text().splitlines() == ["*:1234:prod:bob:oh", "*:*:*:*:eh"]
