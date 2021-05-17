from pglift import model
from pglift.settings import PostgreSQLSettings, Settings


def test_instance_default_version(ctx):
    i = model.Instance.default_version("test", ctx=ctx)
    major_version = str(ctx.pg_ctl(None).version)[:2]
    assert i.version == major_version


def test_instance_config(tmp_path):
    s = Settings(postgresql=PostgreSQLSettings(root=str(tmp_path)))
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

    assert i.config(True) is None
    (datadir / "conf.pglift.d").mkdir()
    (datadir / "conf.pglift.d" / "user.conf").write_text("\n".join(["port=5555"]))
    mconf = i.config(True)
    assert mconf is not None
    assert mconf.port == 5555
