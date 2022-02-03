import sys
from typing import Optional

from . import logger, systemd
from .ctx import BaseContext
from .settings import Settings
from .task import task

POSTGRESQL_SERVICE_NAME = "pglift-postgresql@.service"
POSTGRES_EXPORTER_SERVICE_NAME = "pglift-postgres_exporter@.service"
BACKUP_SERVICE_NAME = "pglift-backup@.service"
BACKUP_TIMER_NAME = "pglift-backup@.timer"


def with_header(content: str, header: str) -> str:
    """Possibly insert `header` on top of `content`.

    >>> print(with_header("blah", "% head"))
    % head
    blah
    >>> with_header("content", "")
    'content'
    """
    if header:
        content = "\n".join([header, content])
    return content


def executeas(settings: Settings) -> str:
    """Return User/Group options for systemd unit depending on settings.

    When systemd.user is set, return an empty string:
    >>> s = Settings.parse_obj({"systemd": {"user": True}})
    >>> executeas(s)
    ''

    Otherwise, user sysuser setting:
    >>> s = Settings.parse_obj({"systemd": {"user": False}, "sysuser": ["postgres", "pgsql"]})
    >>> print(executeas(s))
    User=postgres
    Group=pgsql
    """
    if settings.systemd.user:
        return ""
    user, group = settings.sysuser
    return "\n".join([f"User={user}", f"Group={group}"])


@task("installing systemd template unit for PostgreSQL")
def postgresql_systemd_unit_template(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    settings = ctx.settings.postgresql
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    content = systemd.template(POSTGRESQL_SERVICE_NAME).format(
        executeas=executeas(ctx.settings),
        python=sys.executable,
        environment=environment,
        pid_directory=settings.pid_directory,
    )
    systemd.install(
        POSTGRESQL_SERVICE_NAME,
        with_header(content, header),
        ctx.settings.systemd.unit_path,
        logger=logger,
    )


@postgresql_systemd_unit_template.revert(
    "uninstalling systemd template unit for PostgreSQL"
)
def revert_postgresql_systemd_unit_template(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    systemd.uninstall(
        POSTGRESQL_SERVICE_NAME, ctx.settings.systemd.unit_path, logger=logger
    )


@task("installing systemd template unit for Prometheus postgres_exporter")
def postgres_exporter_systemd_unit_template(
    ctx: BaseContext, *, header: str = ""
) -> None:
    settings = ctx.settings.prometheus
    configpath = str(settings.configpath).replace("{name}", "%i")
    content = systemd.template(POSTGRES_EXPORTER_SERVICE_NAME).format(
        executeas=executeas(ctx.settings),
        configpath=configpath,
        execpath=settings.execpath,
    )
    systemd.install(
        POSTGRES_EXPORTER_SERVICE_NAME,
        with_header(content, header),
        ctx.settings.systemd.unit_path,
        logger=logger,
    )


@postgres_exporter_systemd_unit_template.revert(
    "uninstalling systemd template unit for Prometheus postgres_exporter"
)
def revert_postgres_exporter_systemd_unit_template(
    ctx: BaseContext, *, header: str = ""
) -> None:
    systemd.uninstall(
        POSTGRES_EXPORTER_SERVICE_NAME, ctx.settings.systemd.unit_path, logger=logger
    )


@task("installing systemd template unit and timer for PostgreSQL backups")
def postgresql_backup_systemd_templates(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    service_content = systemd.template(BACKUP_SERVICE_NAME).format(
        executeas=executeas(ctx.settings),
        environment=environment,
        python=sys.executable,
    )
    systemd.install(
        BACKUP_SERVICE_NAME,
        with_header(service_content, header),
        ctx.settings.systemd.unit_path,
        logger=logger,
    )
    timer_content = systemd.template(BACKUP_TIMER_NAME).format(
        # TODO: use a setting for that value
        calendar="daily",
    )
    systemd.install(
        BACKUP_TIMER_NAME,
        with_header(timer_content, header),
        ctx.settings.systemd.unit_path,
        logger=logger,
    )


@postgresql_backup_systemd_templates.revert(
    "uninstalling systemd template unit and timer for PostgreSQL backups"
)
def revert_postgresql_backup_systemd_templates(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    systemd.uninstall(
        BACKUP_SERVICE_NAME, ctx.settings.systemd.unit_path, logger=logger
    )
    systemd.uninstall(BACKUP_TIMER_NAME, ctx.settings.systemd.unit_path, logger=logger)


def do(ctx: BaseContext, env: Optional[str] = None, header: str = "") -> None:
    if ctx.settings.service_manager != "systemd":
        logger.warning("not using systemd as 'service_manager', skipping installation")
        return
    postgresql_systemd_unit_template(ctx, env=env, header=header)
    postgres_exporter_systemd_unit_template(ctx, header=header)
    postgresql_backup_systemd_templates(ctx, env=env, header=header)
    systemd.daemon_reload(ctx)


def undo(ctx: BaseContext) -> None:
    if ctx.settings.service_manager != "systemd":
        logger.warning(
            "not using systemd as 'service_manager', skipping uninstallation"
        )
        return
    revert_postgresql_backup_systemd_templates(ctx)
    revert_postgres_exporter_systemd_unit_template(ctx)
    revert_postgresql_systemd_unit_template(ctx)
    systemd.daemon_reload(ctx)
