from pathlib import Path
from typing import Any, Tuple

import pytest
from pgtoolkit import conf as pgconf

from pglift import conf
from pglift.ctx import Context
from pglift.models.system import Instance
from pglift.settings import Settings


def test_read(pg_version: str, settings: Settings, tmp_path: Path) -> None:
    datadir = tmp_path
    postgresql_auto_conf = datadir / "postgresql.auto.conf"
    postgresql_auto_conf.write_text("primary_conn_info = host=primary\n")
    postgresql_conf = datadir / "postgresql.conf"
    postgresql_conf.write_text("\n".join(["bonjour = hello", "port=1234"]))

    config = conf.read(datadir)
    config.bonjour == "hello"
    config.port == 1234
    config.primary_conn_info == "host=primary"

    config = conf.read(datadir, managed_only=True)
    config.bonjour == "hello"
    config.port == 1234
    assert "primary_conn_info" not in config

    postgresql_conf.unlink()
    with pytest.raises(FileNotFoundError, match=str(postgresql_conf)):
        conf.read(datadir, True)


def test_update(datadir: Path, write_changes: bool) -> None:
    cfg = pgconf.parse(datadir / "postgresql.conf.sample")
    conf.update(
        cfg,
        max_connections=10,  # changed
        bonjour=True,  # uncommented
        log_destination="stderr",  # added
    )
    fpath = datadir / "postgresql.conf"
    if write_changes:
        cfg.save(fpath)
    expected = fpath.read_text().splitlines(keepends=True)
    assert cfg.lines == expected


@pytest.fixture(params=["relative", "absolute"])
def log_directory(
    instance: Instance, request: Any, tmp_path: Path
) -> Tuple[Path, Path]:
    if request.param == "relative":
        path = Path("loghere")
        return path, instance.datadir / path
    else:
        path = tmp_path / "log" / "here"
        return path, path


def test_log_directory(
    ctx: Context, instance: Instance, log_directory: Tuple[Path, Path]
) -> None:
    log_dir, abs_log_dir = log_directory
    assert not abs_log_dir.exists()
    assert conf.log_directory(instance.datadir, log_dir) == abs_log_dir
    abs_log_dir.mkdir(parents=True)
    conf.remove_log_directory(ctx, instance.datadir, log_dir)
    assert not abs_log_dir.exists()
    assert abs_log_dir.parent.exists()
    conf.remove_log_directory(ctx, instance.datadir, log_dir)  # no-op
