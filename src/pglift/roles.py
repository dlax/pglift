from typing import Any, List

import psycopg.pq
import psycopg.rows
from pgtoolkit import conf, pgpass
from psycopg import sql

from . import db, exceptions, hookimpl
from .ctx import BaseContext
from .models import interface
from .models.system import PostgreSQLInstance
from .task import task
from .types import ConfigChanges, Role


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: BaseContext,
    manifest: interface.Instance,
    config: conf.Configuration,
    changes: ConfigChanges,
) -> None:
    """Set / update passfile entry for PostgreSQL roles upon instance
    configuration.

    This handles the entry for super-user role, if configured accordingly.

    If a role should be referenced in password file, we either create an entry
    or update the existing one to reflect configuration changes (e.g. port
    change).
    """

    try:
        old_port, port = changes["port"]
    except KeyError:
        old_port = port = config.get("port", 5432)
    assert isinstance(port, int), port

    surole = interface.instance_surole(ctx.settings, manifest)
    with pgpass.edit(ctx.settings.postgresql.auth.passfile) as passfile:
        surole_entry = None
        if old_port is not None:
            # Port changed, update all entries matching the old value.
            assert isinstance(old_port, int)
            for entry in passfile:
                if entry.matches(port=old_port):
                    if entry.matches(username=surole.name):
                        surole_entry = entry
                    entry.port = port
        if surole.pgpass and surole_entry is None and surole.password:
            # No previous entry for super-user, add one.
            password = surole.password.get_secret_value()
            entry = pgpass.PassEntry("*", port, "*", surole.name, password)
            passfile.lines.append(entry)
            passfile.sort()


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: PostgreSQLInstance) -> None:
    """Remove password file (pgpass) entries for the instance being dropped."""
    passfile_path = ctx.settings.postgresql.auth.passfile
    with pgpass.edit(passfile_path) as passfile:
        passfile.remove(port=instance.port)
    if not passfile.lines:
        passfile_path.unlink()


def apply(
    ctx: BaseContext, instance: PostgreSQLInstance, role_manifest: interface.Role
) -> None:
    """Apply state described by specified role manifest as a PostgreSQL instance.

    The instance should be running.
    """
    if role_manifest.state == interface.PresenceState.absent:
        if exists(ctx, instance, role_manifest.name):
            drop(ctx, instance, role_manifest.name)
        return None

    if not exists(ctx, instance, role_manifest.name):
        create(ctx, instance, role_manifest)
    else:
        alter(ctx, instance, role_manifest)
    set_pgpass_entry_for(ctx, instance, role_manifest)


def describe(
    ctx: BaseContext, instance: PostgreSQLInstance, name: str
) -> interface.Role:
    """Return a role described as a manifest.

    :raises ~pglift.exceptions.RoleNotFound: if no role with specified 'name' exists.
    """
    if not exists(ctx, instance, name):
        raise exceptions.RoleNotFound(name)
    with db.superuser_connect(ctx, instance) as cnx:
        with cnx.cursor(row_factory=psycopg.rows.class_row(interface.Role)) as cur:
            cur.execute(db.query("role_inspect"), {"username": name})
            role = cur.fetchone()
            assert role is not None
    if in_pgpass(ctx, instance, name):
        role.pgpass = True
    return role


@task("dropping role '{name}' from instance {instance}")
def drop(ctx: BaseContext, instance: PostgreSQLInstance, name: str) -> None:
    """Drop a role from instance.

    :raises ~pglift.exceptions.RoleNotFound: if no role with specified 'name' exists.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    if not exists(ctx, instance, name):
        raise exceptions.RoleNotFound(name)
    with db.superuser_connect(ctx, instance) as cnx:
        cnx.execute(db.query("role_drop", username=sql.Identifier(name)))
        cnx.commit()
    role = interface.Role(name=name, pgpass=False)
    set_pgpass_entry_for(ctx, instance, role)


def exists(ctx: BaseContext, instance: PostgreSQLInstance, name: str) -> bool:
    """Return True if named role exists in 'instance'.

    The instance should be running.
    """
    with db.superuser_connect(ctx, instance) as cnx:
        cur = cnx.execute(db.query("role_exists"), {"username": name})
        return cur.rowcount == 1


def has_password(ctx: BaseContext, instance: PostgreSQLInstance, name: str) -> bool:
    """Return True if the role has a password set."""
    with db.superuser_connect(ctx, instance) as cnx:
        cur = cnx.execute(db.query("role_has_password"), {"username": name})
        haspassword = cur.fetchone()["haspassword"]  # type: ignore[index]
        assert isinstance(haspassword, bool)
        return haspassword


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
    with_password: bool = True,
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
    if with_password and role.password is not None:
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
    ctx: BaseContext, instance: PostgreSQLInstance, role: interface.Role
) -> None:
    """Create 'role' in 'instance'.

    The instance should be a running primary and the role should not exist already.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    with db.superuser_connect(ctx, instance) as cnx:
        opts = options(cnx, role)
        cnx.execute(
            db.query("role_create", username=sql.Identifier(role.name), options=opts)
        )
        cnx.commit()


@task("altering role '{role.name}' on instance {instance}")
def alter(ctx: BaseContext, instance: PostgreSQLInstance, role: interface.Role) -> None:
    """Alter 'role' in 'instance'.

    The instance should be running primary and the role should exist already.
    """
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    actual_role = describe(ctx, instance, role.name)
    in_roles = {
        "grant": set(role.in_roles) - set(actual_role.in_roles),
        "revoke": set(actual_role.in_roles) - set(role.in_roles),
    }
    with db.superuser_connect(ctx, instance) as cnx:
        opts = options(
            cnx,
            role,
            with_password=not has_password(ctx, instance, role.name),
            in_roles=False,
        )
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


@task("setting password for '{role.name}' role")
def set_password_for(
    ctx: BaseContext, instance: PostgreSQLInstance, role: Role
) -> None:
    """Set password for a PostgreSQL role on a primary instance."""
    if instance.standby:
        raise exceptions.InstanceReadOnlyError(instance)
    if role.password is None:
        return

    with db.superuser_connect(ctx, instance) as conn:
        conn.autocommit = True
        options = sql.SQL("PASSWORD {}").format(encrypt_password(conn, role))
        conn.execute(
            db.query("role_alter", username=sql.Identifier(role.name), options=options),
        )


def in_pgpass(ctx: BaseContext, instance: PostgreSQLInstance, name: str) -> bool:
    """Return True if a role with 'name' is present in password file for
    'instance'.
    """
    port = int(instance.config().port)  # type: ignore[arg-type]
    passfile = pgpass.parse(ctx.settings.postgresql.auth.passfile)
    return any(entry.matches(username=name, port=port) for entry in passfile)


@task("editing password file entry for '{role.name}' role")
def set_pgpass_entry_for(
    ctx: BaseContext, instance: PostgreSQLInstance, role: interface.Role
) -> None:
    """Add, update or remove a password file entry for 'role' of 'instance'."""

    port = instance.port
    username = role.name
    password = None
    if role.password:
        password = role.password.get_secret_value()
    with pgpass.edit(ctx.settings.postgresql.auth.passfile) as passfile:
        for entry in passfile:
            if entry.matches(username=username, port=port):
                if not role.pgpass:
                    passfile.lines.remove(entry)
                elif password is not None:
                    entry.password = password
                break
        else:
            if role.pgpass and password is not None:
                entry = pgpass.PassEntry("*", port, "*", username, password)
                passfile.lines.append(entry)
                passfile.sort()
