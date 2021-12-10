import subprocess
from typing import Any, List, NoReturn

import pytest

from pglift import postgres
from pglift.ctx import Context
from pglift.models.system import Instance


def test_main_errors() -> None:
    with pytest.raises(SystemExit, match="2"):
        postgres.main(["aa"])
    with pytest.raises(SystemExit, match="2"):
        postgres.main(["12-"])
    with pytest.raises(SystemExit, match="2"):
        postgres.main(["12-test"])


def test_main(
    monkeypatch: pytest.MonkeyPatch, ctx: Context, instance: Instance
) -> None:
    calls = []

    class Popen:
        def __init__(self, cmd: List[str], **kwargs: Any):
            calls.append(cmd)
            self.cmd = cmd
            self.pid = 123
            self.returncode = 0

        def communicate(self, **kwargs: Any) -> NoReturn:
            raise subprocess.TimeoutExpired(self.cmd, 12)

    with monkeypatch.context() as m:
        m.setattr("subprocess.Popen", Popen)
        postgres.main([f"{instance.version}-{instance.name}"], ctx=ctx)
    bindir = ctx.settings.postgresql.versions[instance.version].bindir
    assert calls == [[str(bindir / "postgres"), "-D", str(instance.datadir)]]
    assert (
        ctx.settings.postgresql.pid_directory
        / f"postgresql-{instance.version}-{instance.name}.pid"
    ).read_text() == "123"
