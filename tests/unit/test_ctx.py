from pathlib import Path

import pytest

from pglift import ctx, util
from pglift.settings import Settings


def test_site_config(
    settings: Settings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = ctx.Context(settings=settings)
    configdir = tmp_path / "pglift"
    configdir.mkdir()
    configfile = configdir / "x"
    configfile.touch()
    with monkeypatch.context() as m:
        m.setenv("XDG_CONFIG_HOME", str(tmp_path))
        assert context.site_config("x") == configfile
        pg_hba = context.site_config("postgresql", "pg_hba.conf")
    assert context.site_config("x") is None
    assert pg_hba is not None
    assert pg_hba.parent == util.datapath / "postgresql"
