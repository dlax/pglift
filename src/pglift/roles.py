import psycopg2
from pgtoolkit import pgpass

from . import hookimpl, queries
from .ctx import BaseContext
from .model import Instance
from .settings import Role
from .types import ConfigChanges


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: BaseContext, instance: Instance, changes: ConfigChanges
) -> None:
    """Set / update passfile entry for PostgreSQL roles upon instance
    configuration.

    If a role should be referenced in password file, we either create an entry
    or update the existing one to reflect configuration changes (e.g. port
    change).
    """
    surole = ctx.settings.postgresql.surole
    if surole.pgpass:

        config = instance.config()
        assert config is not None
        try:
            old_port, port = changes["port"]
        except KeyError:
            old_port = port = config.port
        assert isinstance(port, int)

        username = surole.name
        with pgpass.edit(ctx.settings.postgresql.auth.passfile) as passfile:
            entry = None
            if old_port is not None:
                assert isinstance(old_port, int)
                for entry in passfile:
                    if entry.matches(username=username, port=old_port):
                        entry.port = port
                        break
            if entry is None and surole.password:
                password = surole.password.get_secret_value()
                entry = pgpass.PassEntry("*", port, "*", username, password)
                passfile.lines.append(entry)
                passfile.sort()


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: BaseContext, instance: Instance) -> None:
    """Remove password file (pgpass) entries for the instance being dropped."""
    config = instance.config()
    assert config is not None
    passfile_path = ctx.settings.postgresql.auth.passfile
    with pgpass.edit(passfile_path) as passfile:
        passfile.remove(port=config.port)  # type: ignore[arg-type]
    if not passfile.lines:
        passfile_path.unlink()


def set_password_for(ctx: BaseContext, instance: Instance, role: Role) -> None:
    """Set password for a PostgreSQL role on instance."""
    if role.password is None:
        return

    config = instance.config()
    assert config is not None
    password = role.password.get_secret_value()
    connargs = {
        "port": config.port,
        "dbname": "postgres",
        "user": role.name,
    }

    if config.unix_socket_directories:
        connargs["host"] = config.unix_socket_directories
    passfile = ctx.settings.postgresql.auth.passfile
    if role.pgpass and passfile.exists():
        connargs["passfile"] = str(passfile)
    else:
        connargs["password"] = password

    with psycopg2.connect(**connargs) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                queries.get("role_alter_password", username=role.name),
                {"password": password},
            )
