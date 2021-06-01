import pytest

from pglift import postgres


def test_main_errors():
    with pytest.raises(SystemExit, match="2"):
        postgres.main(["aa"])
    with pytest.raises(SystemExit, match="2"):
        postgres.main(["12/"])
    with pytest.raises(SystemExit, match="2"):
        postgres.main(["12/test"])


def test_main(monkeypatch, ctx, instance):
    calls = []

    class Popen:
        def __init__(self, cmd):
            calls.append(cmd)
            self.pid = 123

    with monkeypatch.context() as m:
        m.setattr("subprocess.Popen", Popen)
        postgres.main([str(instance)], ctx=ctx)
    bindir = ctx.settings.postgresql.versions[instance.version].bindir
    assert calls == [[str(bindir / "postgres"), "-D", str(instance.datadir)]]
    assert (
        ctx.settings.postgresql.pid_directory
        / f"postgresql-{instance.version}-{instance.name}.pid"
    ).read_text() == "123"
