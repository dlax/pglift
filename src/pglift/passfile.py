import logging
from typing import TYPE_CHECKING

from pgtoolkit import pgpass

from . import hookimpl

if TYPE_CHECKING:
    from pgtoolkit.conf import Configuration

    from .ctx import BaseContext
    from .models import interface
    from .models.system import PostgreSQLInstance
    from .types import ConfigChanges

logger = logging.getLogger(__name__)


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: "BaseContext",
    manifest: "interface.Instance",
    config: "Configuration",
    changes: "ConfigChanges",
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
    else:
        if port is None:
            port = config.get("port", 5432)
    assert isinstance(port, int), port

    surole = manifest.surole(ctx.settings)
    passfile = ctx.settings.postgresql.auth.passfile
    with pgpass.edit(passfile) as f:
        surole_entry = None
        if old_port is not None:
            # Port changed, update all entries matching the old value.
            assert isinstance(old_port, int)
            for entry in f:
                if entry.matches(port=old_port):
                    if entry.matches(username=surole.name):
                        surole_entry = entry
                    entry.port = port
                    logger.info(
                        "updating entry for '%(username)s' in %(passfile)s (port changed)",
                        {"username": surole.name, "passfile": passfile},
                    )
        if surole.pgpass and surole_entry is None and surole.password:
            # No previous entry for super-user, add one.
            password = surole.password.get_secret_value()
            entry = pgpass.PassEntry("*", port, "*", surole.name, password)
            f.lines.append(entry)
            logger.info(
                "adding an entry for '%(username)s' in %(passfile)s",
                {"username": surole.name, "passfile": passfile},
            )
            f.sort()


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: "BaseContext", instance: "PostgreSQLInstance") -> None:
    """Remove password file (pgpass) entries for the instance being dropped."""
    passfile_path = ctx.settings.postgresql.auth.passfile
    with pgpass.edit(passfile_path) as passfile:
        logger.info(
            "removing entries matching port=%(port)s from %(passfile)s",
            {"port": instance.port, "passfile": passfile_path},
        )
        passfile.remove(port=instance.port)
    if not passfile.lines:
        logger.info(
            "removing now empty %(passfile)s",
            {"passfile": passfile_path},
        )
        passfile_path.unlink()
