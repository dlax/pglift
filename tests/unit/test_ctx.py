from pglift.ctx import Context


def test_libpq_environ(ctx: Context) -> None:
    assert ctx.libpq_environ(base={}) == {
        "PGPASSFILE": str(ctx.settings.postgresql.auth.passfile)
    }
    assert ctx.libpq_environ(base={"PGPASSFILE": "/var/lib/pgsql/pgpass"}) == {
        "PGPASSFILE": "/var/lib/pgsql/pgpass"
    }
