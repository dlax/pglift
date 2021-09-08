import sys
from typing import Optional

from . import systemd
from .ctx import BaseContext
from .task import runner, task


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


@task
def postgresql_systemd_unit_template(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
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
        with_header(content, header),
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )


@postgresql_systemd_unit_template.revert
def revert_postgresql_systemd_unit_template(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    systemd.uninstall("postgresql@.service", ctx.settings.systemd.unit_path, logger=ctx)


@task
def postgres_exporter_systemd_unit_template(
    ctx: BaseContext, *, header: str = ""
) -> None:
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
        with_header(content, header),
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )


@postgres_exporter_systemd_unit_template.revert
def revert_postgres_exporter_systemd_unit_template(
    ctx: BaseContext, *, header: str = ""
) -> None:
    systemd.uninstall(
        "postgres_exporter@.service", ctx.settings.systemd.unit_path, logger=ctx
    )


@task
def postgresql_backup_systemd_templates(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
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
        with_header(service_content, header),
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )
    timer_content = systemd.template("postgresql-backup.timer").format(
        # TODO: use a setting for that value
        calendar="daily",
    )
    systemd.install(
        "postgresql-backup@.timer",
        with_header(timer_content, header),
        ctx.settings.systemd.unit_path,
        logger=ctx,
    )


@postgresql_backup_systemd_templates.revert
def revert_postgresql_backup_systemd_templates(
    ctx: BaseContext, *, env: Optional[str] = None, header: str = ""
) -> None:
    systemd.uninstall(
        "postgresql-backup@.service", ctx.settings.systemd.unit_path, logger=ctx
    )
    systemd.uninstall(
        "postgresql-backup@.timer", ctx.settings.systemd.unit_path, logger=ctx
    )


def do(ctx: BaseContext, env: Optional[str] = None, header: str = "") -> None:
    if ctx.settings.service_manager != "systemd":
        ctx.warning("not using systemd as 'service_manager', skipping installation")
        return
    with runner(ctx):
        postgresql_systemd_unit_template(ctx, env=env, header=header)
        postgres_exporter_systemd_unit_template(ctx, header=header)
        postgresql_backup_systemd_templates(ctx, env=env, header=header)
        systemd.daemon_reload(ctx)


def undo(ctx: BaseContext) -> None:
    if ctx.settings.service_manager != "systemd":
        return
    with runner(ctx):
        revert_postgresql_backup_systemd_templates(ctx)
        revert_postgres_exporter_systemd_unit_template(ctx)
        revert_postgresql_systemd_unit_template(ctx)
        systemd.daemon_reload(ctx)
