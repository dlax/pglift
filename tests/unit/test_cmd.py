import logging
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from pglift import cmd
from pglift.exceptions import CommandError, SystemError


def test_execute_program(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    command = ["/c", "m", "d"]
    with patch("os.execve") as execve, patch("os.execv") as execv:
        cmd.execute_program(command, env={"X": "Y"})
        execve.assert_called_once_with("/c", command, {"X": "Y"})
        assert not execv.called
    logger = logging.getLogger(__name__)
    with patch("os.execve") as execve, patch("os.execv") as execv, caplog.at_level(
        logging.DEBUG, logger=__name__
    ):
        cmd.execute_program(command, logger=logger)
        execv.assert_called_once_with("/c", command)
        assert not execve.called
    assert "executing program /c m d" in caplog.records[0].message


def test_start_program_terminate_program_status_program(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    logger = logging.getLogger(__name__)

    pidfile = tmp_path / "sleep" / "pid"
    cmd.start_program(
        ["sleep", "10"], pidfile, timeout=0.01, env={"X_DEBUG": "1"}, logger=logger
    )
    with pidfile.open() as f:
        pid = f.read()

    proc = Path("/proc") / pid
    assert proc.exists()
    assert "sleep\x0010\x00" in (proc / "cmdline").read_text()
    assert "X_DEBUG" in (proc / "environ").read_text()

    assert cmd.status_program(pidfile) == cmd.Status.running

    with pytest.raises(SystemError, match="running already"):
        cmd.start_program(["sleep", "10"], pidfile, logger=logger)

    cmd.terminate_program(pidfile, logger=logger)
    r = subprocess.run(["pgrep", pid], check=False)
    assert r.returncode == 1

    assert cmd.status_program(pidfile) == cmd.Status.not_running

    pidfile = tmp_path / "invalid.pid"
    pidfile.write_text("innnnvaaaaaaaaaaliiiiiiiiiiid")
    assert cmd.status_program(pidfile) == cmd.Status.dangling
    caplog.clear()
    with pytest.raises(CommandError), caplog.at_level(logging.WARNING, logger=__name__):
        cmd.start_program(
            ["sleep", "well"], pidfile, logger=logger, env={"LANG": "C", "LC_ALL": "C"}
        )
    assert not pidfile.exists()
    assert "sleep is supposed to be running" in caplog.records[0].message
    assert "sleep: invalid time interval 'well'" in caplog.records[1].message

    pidfile = tmp_path / "notfound"
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=__name__):
        cmd.terminate_program(pidfile, logger=logger)
    assert f"program from {pidfile} not running" in caplog.records[0].message
