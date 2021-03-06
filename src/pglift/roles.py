import logging
from typing import TYPE_CHECKING, Any, List, Optional

import psycopg.pq
import psycopg.rows
from pgtoolkit import pgpass
from psycopg import sql

from . import db, exceptions
from .models import interface
from .task import task
from .types import Role

if TYPE_CHECKING:
    from .ctx import BaseContext
    from .models import system

logger = logging.getLogger(__name__)


def apply(
    ctx: "BaseContext", instance: "system.PostgreSQLInstance", role: interface.Role
) -> Optional[bool]:
    """Apply state described by specified interface model as a PostgreSQL role.

    Return True, if changes were applied, False if no change is needed, and
    None if the role got dropped. In case it's not possible to inspect changed
    role, possibly due to the super-user password being modified, return True
    with a warning logged.

    The instance should be running.
    """
    name = role.name
    if role.state == interface.PresenceState.absent:
        if exists(ctx, instance, name):
            drop(ctx, instance, name)
            return None
        return False

    if not exists(ctx, instance, name):
        create(ctx, instance, role)
        set_pgpass_entry_for(ctx, instance, role)
        return True
    else:
        actual = get(ctx, instance, name, password=False)
        alter(ctx, instance, role)
        if set_pgpass_entry_for(ctx, instance, role):
            return True
        try:
            return get(ctx, instance, name, password=False) != actual
        except psycopg.OperationalError as e:
            logger.warning(
                "failed to retrieve new role %s, possibly due to password being modified: %s",
                name,
                e,
            )
            return True


def get(
    ctx: "BaseContext",
    instance: "system.PostgreSQLInstance",
    name: str,
    *,
    password: bool = True,
) -> interface.Role:
    """Return the role object with specified name.

    :raises ~pglift.exceptions.RoleNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.RoleNotFound(name)
    with db.connect(ctx, instance) as cnx:
        values = cnx.execute(db.query("role_inspect"), {"username": name}).fetchone()
        assert values is not None
    if in_pgpass(ctx, instance, name):
        values["pgpass"] = True
    if not password:
        values["password"] = None
    return interface.Role(**values)


@task("dropping role '{name}' from instance {instance}")
def drop(ctx: "BaseContext", instance: "system.PostgreSQLInstance", name: str) -> None:
    """Drop a role from instance.

    :raises ~pglift.exceptions.RoleNotFound: if no role with specified 'name' exists.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    if not exists(ctx, instance, name):
        raise exceptions.RoleNotFound(name)
    with db.connect(ctx, instance) as cnx:
        cnx.execute(db.query("role_drop", username=sql.Identifier(name)))
        cnx.commit()
    role = interface.Role(name=name, pgpass=False)
    set_pgpass_entry_for(ctx, instance, role)


def exists(
    ctx: "BaseContext", instance: "system.PostgreSQLInstance", name: str
) -> bool:
    """Return True if named role exists in 'instance'.

    The instance should be running.
    """
    with db.connect(ctx, instance) as cnx:
        cur = cnx.execute(db.query("role_exists"), {"username": name})
        return cur.rowcount == 1


def encrypt_password(cnx: psycopg.Connection[Any], role: Role) -> str:
    assert role.password is not None, "role has no password to encrypt"
    encoding = cnx.info.encoding
    return cnx.pgconn.encrypt_password(
        role.password.get_secret_value().encode(encoding), role.name.encode(encoding)
    ).decode(encoding)


def options(
    cnx: psycopg.Connection[Any],
    role: interface.Role,
    *,
    in_roles: bool = True,
) -> sql.Composable:
    """Return the "options" part of CREATE ROLE or ALTER ROLE SQL commands
    based on 'role' model.
    """
    opts: List[sql.Composable] = [
        sql.SQL("INHERIT" if role.inherit else "NOINHERIT"),
        sql.SQL("LOGIN" if role.login else "NOLOGIN"),
        sql.SQL("SUPERUSER" if role.superuser else "NOSUPERUSER"),
        sql.SQL("REPLICATION" if role.replication else "NOREPLICATION"),
    ]
    if role.password is not None:
        opts.append(sql.SQL("PASSWORD {}").format(encrypt_password(cnx, role)))
    if role.validity is not None:
        opts.append(sql.SQL("VALID UNTIL {}").format(role.validity.isoformat()))
    opts.append(
        sql.SQL(
            "CONNECTION LIMIT {}".format(
                role.connection_limit if role.connection_limit is not None else -1
            )
        )
    )
    if in_roles and role.in_roles:
        opts.append(
            sql.SQL(" ").join(
                [
                    sql.SQL("IN ROLE"),
                    sql.SQL(", ").join(
                        sql.Identifier(in_role) for in_role in role.in_roles
                    ),
                ]
            )
        )
    return sql.SQL(" ").join(opts)


@task("creating role '{role.name}' on instance {instance}")
def create(
    ctx: "BaseContext", instance: "system.PostgreSQLInstance", role: interface.Role
) -> None:
    """Create 'role' in 'instance'.

    The instance should be a running primary and the role should not exist already.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    with db.connect(ctx, instance) as cnx:
        opts = options(cnx, role)
        cnx.execute(
            db.query("role_create", username=sql.Identifier(role.name), options=opts)
        )
        cnx.commit()


@task("altering role '{role.name}' on instance {instance}")
def alter(
    ctx: "BaseContext", instance: "system.PostgreSQLInstance", role: interface.Role
) -> None:
    """Alter 'role' in 'instance'.

    The instance should be running primary and the role should exist already.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    actual_role = get(ctx, instance, role.name)
    in_roles = {
        "grant": set(role.in_roles) - set(actual_role.in_roles),
        "revoke": set(actual_role.in_roles) - set(role.in_roles),
    }
    with db.connect(ctx, instance) as cnx:
        opts = options(cnx, role, in_roles=False)
        cnx.execute(
            db.query(
                "role_alter",
                username=sql.Identifier(role.name),
                options=opts,
            ),
        )
        for action, values in in_roles.items():
            if values:
                cnx.execute(
                    db.query(
                        f"role_{action}",
                        rolname=sql.SQL(", ").join(sql.Identifier(r) for r in values),
                        rolspec=sql.Identifier(role.name),
                    )
                )
        cnx.commit()


def set_password_for(
    ctx: "BaseContext", instance: "system.PostgreSQLInstance", role: Role
) -> None:
    """Set password for a PostgreSQL role on a primary instance."""
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    if role.password is None:
        return

    logger.info("setting password for '%(username)s' role", {"username": role.name})
    with db.connect(ctx, instance) as conn:
        conn.autocommit = True
        options = sql.SQL("PASSWORD {}").format(encrypt_password(conn, role))
        conn.execute(
            db.query("role_alter", username=sql.Identifier(role.name), options=options),
        )


def in_pgpass(
    ctx: "BaseContext", instance: "system.PostgreSQLInstance", name: str
) -> bool:
    """Return True if a role with 'name' is present in password file for
    'instance'.
    """
    passfile_path = ctx.settings.postgresql.auth.passfile
    if not passfile_path.exists():
        return False
    port = int(instance.config().port)  # type: ignore[arg-type]
    passfile = pgpass.parse(passfile_path)
    return any(entry.matches(username=name, port=port) for entry in passfile)


def set_pgpass_entry_for(
    ctx: "BaseContext", instance: "system.PostgreSQLInstance", role: interface.Role
) -> bool:
    """Add, update or remove a password file entry for 'role' of 'instance'.

    Return True if any change got applied.
    """

    port = instance.port
    username = role.name
    password = None
    if role.password:
        password = role.password.get_secret_value()
    passfile = ctx.settings.postgresql.auth.passfile
    with pgpass.edit(passfile) as f:
        for entry in f:
            if entry.matches(username=username, port=port):
                if not role.pgpass:
                    logger.info(
                        "removing entry for '%(username)s' in %(passfile)s",
                        {"username": username, "passfile": passfile},
                    )
                    f.lines.remove(entry)
                    return True
                elif password is not None:
                    logger.info(
                        "updating password for '%(username)s' in %(passfile)s",
                        {"username": username, "passfile": passfile},
                    )
                    entry.password = password
                    return True
                return False
        else:
            if role.pgpass and password is not None:
                logger.info(
                    "adding an entry for '%(username)s' in %(passfile)s",
                    {"username": username, "passfile": passfile},
                )
                entry = pgpass.PassEntry("*", port, "*", username, password)
                f.lines.append(entry)
                f.sort()
                return True
            return False
