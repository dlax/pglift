from typing import List, Sequence

from psycopg2 import sql

from . import db
from .ctx import BaseContext
from .models import interface
from .models.system import Instance


def inspect_default_acl(
    ctx: BaseContext, instance: Instance, database: str, roles: Sequence[str] = ()
) -> List[interface.Privilege]:
    args = {}
    where_clause = sql.SQL("")
    if roles:
        where_clause = sql.SQL("WHERE pg_roles.rolname IN %(roles)s")
        args["roles"] = tuple(roles)
    with db.superuser_connect(instance, dbname=database) as cnx:
        with cnx.cursor() as cur:
            cur.execute(
                db.query("database_default_acl", where_clause=where_clause), args
            )
            results = cur.fetchall()
    return [interface.Privilege(**r) for r in results]


def get(
    ctx: BaseContext,
    instance: Instance,
    *,
    databases: Sequence[str] = (),
    roles: Sequence[str] = (),
) -> List[interface.Privilege]:
    """List default access privileges for databases of an instance.

    :param databases: list of databases to inspect (all will be inspected if
        unspecified).
    :param roles: list of roles to restrict inspection on.

    :raises ValueError: if an element of `databases` or `roles` does not
        exist.
    """

    with db.superuser_connect(instance) as cnx:
        with cnx.cursor() as cur:
            cur.execute(db.query("database_list"))
            existing_databases = [db["name"] for db in cur.fetchall()]
    if not databases:
        databases = existing_databases
    else:
        unknown_dbs = set(databases) - set(existing_databases)
        if unknown_dbs:
            raise ValueError(f"database(s) not found: {', '.join(unknown_dbs)}")

    if roles:
        with db.superuser_connect(instance) as cnx:
            with cnx.cursor() as cur:
                cur.execute(db.query("role_list_names"))
                existing_roles = [n for n, in cur.fetchall()]
        unknown_roles = set(roles) - set(existing_roles)
        if unknown_roles:
            raise ValueError(f"role(s) not found: {', '.join(unknown_roles)}")

    return [
        prvlg
        for database in databases
        for prvlg in inspect_default_acl(ctx, instance, database, roles=roles)
    ]
