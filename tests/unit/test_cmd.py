import logging
import subprocess
from pathlib import Path

import pytest

from pglift import cmd


def test_execute_program_terminate_program(caplog, tmp_path):
    pidfile = tmp_path / "sleep" / "pid"
    cmd.execute_program(["sleep", "10"], pidfile, timeout=0.01, env={"X_DEBUG": "1"})
    with pidfile.open() as f:
        pid = f.read()

    proc = Path("/proc") / pid
    assert proc.exists()
    assert (proc / "cmdline").read_text() == "sleep\x0010\x00"
    assert "X_DEBUG" in (proc / "environ").read_text()

    cmd.terminate_program(pidfile)
    r = subprocess.run(["pgrep", pid], check=False)
    assert r.returncode == 1

    pidfile = tmp_path / "invalid.pid"
    with pytest.raises(subprocess.CalledProcessError), caplog.at_level(
        logging.ERROR, logger=__name__
    ):
        cmd.execute_program(
            ["sleep", "well"], pidfile, logger=logging.getLogger(__name__)
        )
    assert not pidfile.exists()
    assert "sleep: invalid time interval ‘well’" in caplog.records[0].message
