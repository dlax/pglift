import logging
import subprocess
from pathlib import Path

import pytest

from pglift import cmd
from pglift.exceptions import CommandError, FileNotFoundError, SystemError


def test_execute_program_terminate_program(caplog, tmp_path):
    logger = logging.getLogger(__name__)

    pidfile = tmp_path / "sleep" / "pid"
    cmd.execute_program(
        ["sleep", "10"], pidfile, timeout=0.01, env={"X_DEBUG": "1"}, logger=logger
    )
    with pidfile.open() as f:
        pid = f.read()

    proc = Path("/proc") / pid
    assert proc.exists()
    assert (proc / "cmdline").read_text() == "sleep\x0010\x00"
    assert "X_DEBUG" in (proc / "environ").read_text()

    with pytest.raises(SystemError, match="running already"):
        cmd.execute_program(["sleep", "10"], pidfile, logger=logger)

    cmd.terminate_program(pidfile, logger=logger)
    r = subprocess.run(["pgrep", pid], check=False)
    assert r.returncode == 1

    pidfile = tmp_path / "invalid.pid"
    pidfile.write_text("innnnvaaaaaaaaaaliiiiiiiiiiid")
    with pytest.raises(CommandError), caplog.at_level(logging.WARNING, logger=__name__):
        cmd.execute_program(["sleep", "well"], pidfile, logger=logger)
    assert not pidfile.exists()
    assert "sleep is supposed to be running" in caplog.records[0].message
    assert "sleep: invalid time interval ‘well’" in caplog.records[1].message

    pidfile = tmp_path / "notfound"
    with pytest.raises(FileNotFoundError):
        cmd.terminate_program(pidfile, logger=logger)
