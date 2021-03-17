from pglib import model, settings


def test_instance_default_version(ctx):
    i = model.Instance.default_version("test", ctx=ctx)
    assert str(i) == "11/test"


def test_instance_config(tmp_path):
    s = settings.to_config({"PGLIB_POSTGRESQL_ROOT": str(tmp_path)})
    assert s.postgresql.root == tmp_path

    i = model.Instance("test", "12", settings=s)
    assert i.config() is None

    datadir = tmp_path / i.version / i.name / "data"
    datadir.mkdir(parents=True)
    (datadir / "postgresql.conf").write_text(
        "\n".join(["bonjour = hello", "port=1234"])
    )

    config = i.config()
    assert config is not None
    config.bonjour == "hello"
    config.port == 1234
