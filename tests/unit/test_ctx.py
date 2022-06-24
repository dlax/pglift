import logging
from pathlib import Path

import pytest

from pglift import ctx, util
from pglift.settings import Settings


def test_rmtree(
    settings: Settings, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    context = ctx.Context(settings=settings)
    d1 = tmp_path / "d1"
    d1.mkdir()
    d2 = tmp_path / "d2"
    d2.symlink_to(d1, target_is_directory=True)
    with caplog.at_level(logging.WARNING):
        context.rmtree(d2)
    assert (
        f"failed to delete {d2} during tree deletion of {d2}: Cannot call rmtree on a symbolic link"
        in caplog.messages
    )

    caplog.clear()

    with caplog.at_level(logging.WARNING):
        context.rmtree(d1)
    assert not caplog.messages


def test_site_config(
    settings: Settings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = ctx.Context(settings=settings)
    scontext = ctx.SiteContext(settings=settings)
    configdir = tmp_path / "pglift"
    configdir.mkdir()
    configfile = configdir / "x"
    configfile.touch()
    assert context.site_config("x") is None
    pg_hba = context.site_config("postgresql", "pg_hba.conf")
    assert pg_hba is not None
    assert pg_hba.parent == util.datapath / "postgresql"
    with monkeypatch.context() as m:
        m.setenv("XDG_CONFIG_HOME", str(tmp_path))
        assert scontext.site_config("x") == configfile
        pg_hba = scontext.site_config("postgresql", "pg_hba.conf")
    assert scontext.site_config("x") is None
    assert pg_hba is not None
    assert pg_hba.parent == util.datapath / "postgresql"
