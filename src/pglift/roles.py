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
    """Add an entry for PostgreSQL roles upon instance configuration."""
    surole = ctx.settings.postgresql.surole

    if surole.pgpass and surole.password:
        config = instance.config()
        assert config is not None
        port = config.port

        username = surole.name
        password = surole.password.get_secret_value()
        with pgpass.edit(ctx.settings.postgresql.auth.passfile) as passfile:
            entry = pgpass.PassEntry.parse(f"*:{port}:*:{username}:{password}")
            passfile.lines.append(entry)
            passfile.sort()


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
