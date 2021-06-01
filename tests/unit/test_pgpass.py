from pglift import pgpass


def test_add(tmp_path):
    passfile = tmp_path / "pgpass"
    pgpass.add(passfile, "ah")
    assert passfile.read_text().splitlines() == ["*:*:*:*:ah"]
    pgpass.add(passfile, "oh", username="bob", database="prod", port=1234)
    assert passfile.read_text().splitlines() == ["*:1234:prod:bob:oh", "*:*:*:*:ah"]
    pgpass.add(password="eh", passfile=passfile)
    assert passfile.read_text().splitlines() == ["*:1234:prod:bob:oh", "*:*:*:*:eh"]


def test_remove(tmp_path):
    passfile = tmp_path / "pgpass"
    passfile.write_text("\n".join(["host:123:*:user:blah", "*:5432:*:dba:pif"]))
    pgpass.remove(passfile, port=123, username="user")
    assert passfile.read_text() == "*:5432:*:dba:pif\n"
    pgpass.remove(passfile, port=5432)
    assert not passfile.exists()
