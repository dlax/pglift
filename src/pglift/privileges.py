from typing import TYPE_CHECKING, List, Sequence, Union

import psycopg.rows
from psycopg import sql

from . import db
from .ctx import BaseContext
from .models import interface

if TYPE_CHECKING:
    from .models import system


def inspect_privileges(
    ctx: BaseContext,
    instance: "system.Instance",
    database: str,
    roles: Sequence[str] = (),
    defaults: bool = False,
) -> Union[List[interface.DefaultPrivilege], List[interface.Privilege]]:
    args = {}
    where_clause = sql.SQL("")
    if roles:
        where_clause = sql.SQL("AND pg_roles.rolname = ANY(%(roles)s)")
        args["roles"] = list(roles)
    if defaults:
        privilege_query = "database_default_acl"
        return_class = interface.DefaultPrivilege
    else:
        privilege_query = "database_privileges"
        return_class = interface.Privilege
    with db.connect(ctx, instance, dbname=database) as cnx:
        with cnx.cursor(row_factory=psycopg.rows.class_row(return_class)) as cur:
            cur.execute(db.query(privilege_query, where_clause=where_clause), args)
            return cur.fetchall()


def get(
    ctx: BaseContext,
    instance: "system.Instance",
    *,
    databases: Sequence[str] = (),
    roles: Sequence[str] = (),
    defaults: bool = False,
) -> Union[List[interface.DefaultPrivilege], List[interface.Privilege]]:
    """List access privileges for databases of an instance.

    :param databases: list of databases to inspect (all will be inspected if
        unspecified).
    :param roles: list of roles to restrict inspection on.
    :param defaults: if ``True``, get default privileges.

    :raises ValueError: if an element of `databases` or `roles` does not
        exist.
    """

    with db.connect(ctx, instance) as cnx:
        cur = cnx.execute(db.query("database_list", where_clause=sql.SQL("")))
        existing_databases = [db["name"] for db in cur.fetchall()]
    if not databases:
        databases = existing_databases
    else:
        unknown_dbs = set(databases) - set(existing_databases)
        if unknown_dbs:
            raise ValueError(f"database(s) not found: {', '.join(unknown_dbs)}")

    if roles:
        with db.connect(ctx, instance) as cnx:
            cur = cnx.execute(db.query("role_list_names"))
            existing_roles = [n["rolname"] for n in cur.fetchall()]
        unknown_roles = set(roles) - set(existing_roles)
        if unknown_roles:
            raise ValueError(f"role(s) not found: {', '.join(unknown_roles)}")

    return [
        prvlg
        for database in databases
        for prvlg in inspect_privileges(
            ctx, instance, database, roles=roles, defaults=defaults
        )
    ]
