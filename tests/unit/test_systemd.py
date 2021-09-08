import logging

import pytest

from pglift import systemd


def test_install_uninstall(tmp_path):
    logger = logging.getLogger(__name__)
    systemd.install("foo", "ahah", tmp_path, logger=logger)
    unit_path = tmp_path / "foo"
    mtime = unit_path.stat().st_mtime
    assert unit_path.read_text() == "ahah"
    systemd.install("foo", "ahah", tmp_path, logger=logger)
    assert unit_path.stat().st_mtime == mtime
    with pytest.raises(FileExistsError, match="not overwriting"):
        systemd.install("foo", "ahahah", tmp_path, logger=logger)
    systemd.uninstall("foo", tmp_path, logger=logger)
    assert not unit_path.exists()
    systemd.uninstall("foo", tmp_path, logger=logger)  # no-op
