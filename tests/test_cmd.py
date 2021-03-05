import subprocess

import pytest

from pglib import cmd


def test_run():
    r = cmd.run(["echo", "ahah"])
    assert r.returncode == 0
    assert r.stdout == "ahah\n"

    with pytest.raises(subprocess.CalledProcessError):
        cmd.run(["false"], check=True)
