from pglift import db
from pglift.instance import running


def test_connect(ctx, instance):
    with running(ctx, instance):
        with db.connect(instance, ctx.settings.postgresql.surole) as cnx:
            with cnx.cursor() as cur:
                cur.execute("select 1")
                (r,) = cur.fetchone()
    assert r == 1
