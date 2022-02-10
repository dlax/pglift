import logging
import sys
from typing import Optional

from . import systemd, util
from .ctx import BaseContext
from .task import task

logger = logging.getLogger(__name__)
POSTGRESQL_SERVICE_NAME = "pglift-postgresql@.service"
BACKUP_SERVICE_NAME = "pglift-backup@.service"
BACKUP_TIMER_NAME = "pglift-backup@.timer"


@task("installing systemd template unit for PostgreSQL")
def postgresql_systemd_unit_template(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    settings = ctx.settings.postgresql
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    content = systemd.template(POSTGRESQL_SERVICE_NAME).format(
        executeas=systemd.executeas(ctx.settings),
        python=sys.executable,
        environment=environment,
        pid_directory=settings.pid_directory,
    )
    systemd.install(
        POSTGRESQL_SERVICE_NAME,
        util.with_header(content, header),
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


@task("installing systemd template unit and timer for PostgreSQL backups")
def postgresql_backup_systemd_templates(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    service_content = systemd.template(BACKUP_SERVICE_NAME).format(
        executeas=systemd.executeas(ctx.settings),
        environment=environment,
        python=sys.executable,
    )
    systemd.install(
        BACKUP_SERVICE_NAME,
        util.with_header(service_content, header),
        ctx.settings.systemd.unit_path,
        logger=logger,
    )
    timer_content = systemd.template(BACKUP_TIMER_NAME).format(
        # TODO: use a setting for that value
        calendar="daily",
    )
    systemd.install(
        BACKUP_TIMER_NAME,
        util.with_header(timer_content, header),
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
    ctx.hook.install_systemd_unit_template(ctx=ctx, header=header)
    postgresql_backup_systemd_templates(ctx, env=env, header=header)
    systemd.daemon_reload(ctx)


def undo(ctx: BaseContext) -> None:
    if ctx.settings.service_manager != "systemd":
        logger.warning(
            "not using systemd as 'service_manager', skipping uninstallation"
        )
        return
    revert_postgresql_backup_systemd_templates(ctx)
    ctx.hook.uninstall_systemd_unit_template(ctx=ctx)
    revert_postgresql_systemd_unit_template(ctx)
    systemd.daemon_reload(ctx)
