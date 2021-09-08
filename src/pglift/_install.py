import sys
from typing import Optional

from . import systemd
from .ctx import BaseContext
from .task import runner, task


@task
def postgresql_systemd_unit_template(
    ctx: BaseContext, *, env: Optional[str] = None
) -> None:
    settings = ctx.settings.postgresql
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    content = systemd.template("postgresql.service").format(
        python=sys.executable,
        environment=environment,
        pid_directory=settings.pid_directory,
    )
    systemd.install(
        "postgresql@.service",
        content,
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )


@postgresql_systemd_unit_template.revert
def revert_postgresql_systemd_unit_template(
    ctx: BaseContext, *, env: Optional[str] = None
) -> None:
    systemd.uninstall("postgresql@.service", ctx.settings.systemd.unit_path, logger=ctx)


@task
def postgres_exporter_systemd_unit_template(ctx: BaseContext) -> None:
    settings = ctx.settings.prometheus
    configpath = str(settings.configpath).replace(
        "{instance.version}-{instance.name}", "%i"
    )
    content = systemd.template("postgres_exporter.service").format(
        configpath=configpath,
        execpath=settings.execpath,
    )
    systemd.install(
        "postgres_exporter@.service",
        content,
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )


@postgres_exporter_systemd_unit_template.revert
def revert_postgres_exporter_systemd_unit_template(ctx: BaseContext) -> None:
    systemd.uninstall(
        "postgres_exporter@.service", ctx.settings.systemd.unit_path, logger=ctx
    )


@task
def postgresql_backup_systemd_templates(
    ctx: BaseContext, *, env: Optional[str] = None
) -> None:
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    service_content = systemd.template("postgresql-backup.service").format(
        environment=environment,
        python=sys.executable,
    )
    systemd.install(
        "postgresql-backup@.service",
        service_content,
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )
    timer_content = systemd.template("postgresql-backup.timer").format(
        # TODO: use a setting for that value
        calendar="daily",
    )
    systemd.install(
        "postgresql-backup@.timer",
        timer_content,
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )


@postgresql_backup_systemd_templates.revert
def revert_postgresql_backup_systemd_templates(
    ctx: BaseContext, *, env: Optional[str] = None
) -> None:
    systemd.uninstall(
        "postgresql-backup@.service", ctx.settings.systemd.unit_path, logger=ctx
    )
    systemd.uninstall(
        "postgresql-backup@.timer", ctx.settings.systemd.unit_path, logger=ctx
    )


def do(ctx: BaseContext, env: Optional[str] = None) -> None:
    if ctx.settings.service_manager != "systemd":
        ctx.warning("not using systemd as 'service_manager', skipping installation")
        return
    with runner(ctx):
        postgresql_systemd_unit_template(ctx, env=env)
        postgres_exporter_systemd_unit_template(ctx)
        postgresql_backup_systemd_templates(ctx, env=env)
        systemd.daemon_reload(ctx)


def undo(ctx: BaseContext) -> None:
    if ctx.settings.service_manager != "systemd":
        return
    with runner(ctx):
        revert_postgresql_backup_systemd_templates(ctx)
        revert_postgres_exporter_systemd_unit_template(ctx)
        revert_postgresql_systemd_unit_template(ctx)
        systemd.daemon_reload(ctx)
