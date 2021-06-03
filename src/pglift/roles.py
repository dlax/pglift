import psycopg2

from . import pgpass, queries
from .ctx import BaseContext
from .model import Instance
from .settings import Role


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


def set_passfile_entry_for(ctx: BaseContext, instance: Instance, role: Role) -> None:
    """Set entry in PostgreSQL passfile (.pgpass) for role of instance."""
    surole = ctx.settings.postgresql.surole

    if surole.password is not None:
        config = instance.config()
        assert config is not None
        password = surole.password.get_secret_value()
        if surole.pgpass:
            pgpass.add(
                ctx.settings.postgresql.auth.passfile,
                password,
                port=config.port,  # type: ignore[arg-type]
                username=surole.name,
            )
