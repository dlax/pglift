from typing import List

import pytest

from pglift.ctx import Context
from pglift.models import system
from pglift.models.interface import Role

from . import execute


@pytest.fixture(scope="session", autouse=True)
def powa_available(powa_available: bool) -> bool:
    if not powa_available:
        pytest.skip("powa is not available")
    return powa_available


def test_powa(
    ctx: Context,
    instance: system.Instance,
    powa_password: str,
) -> None:
    config = instance.config()
    assert (
        config.shared_preload_libraries
        == "passwordcheck, pg_qualstats, pg_stat_statements, pg_stat_kcache"
    )

    powa_settings = ctx.settings.powa
    assert powa_settings is not None
    dbname = powa_settings.dbname

    def get_installed_extensions() -> List[str]:
        return [
            r["extname"]
            for r in execute(
                ctx,
                instance,
                "SELECT extname FROM pg_extension",
                dbname=dbname,
            )
        ]

    installed = get_installed_extensions()
    assert "pg_stat_statements" in installed
    assert "btree_gist" in installed
    assert "powa" in installed

    (record,) = execute(
        ctx,
        instance,
        "SELECT datname from powa_databases_src(0) LIMIT 1",
        fetch=True,
        role=Role(name=powa_settings.role, password=powa_password),
        dbname=dbname,
    )
    assert record["datname"] is not None
