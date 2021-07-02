import sys
from typing import Optional

from . import systemd
from .ctx import BaseContext
from .settings import PostgreSQLSettings, PrometheusSettings
from .task import runner, task


@task
def postgresql_systemd_unit_template(
    settings: PostgreSQLSettings, *, env: Optional[str] = None
) -> None:
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    content = systemd.template("postgresql.service").format(
        python=sys.executable,
        environment=environment,
        pid_directory=settings.pid_directory,
    )
    systemd.install("postgresql@.service", content)


@postgresql_systemd_unit_template.revert
def revert_postgresql_systemd_unit_template(
    settings: PostgreSQLSettings, *, env: Optional[str] = None
) -> None:
    systemd.uninstall("postgresql@.service")


@task
def postgres_exporter_systemd_unit_template(settings: PrometheusSettings) -> None:
    configpath = str(settings.configpath).replace(
        "{instance.version}-{instance.name}", "%i"
    )
    content = systemd.template("postgres_exporter.service").format(
        configpath=configpath,
        execpath=settings.execpath,
    )
    systemd.install("postgres_exporter@.service", content)


@postgres_exporter_systemd_unit_template.revert
def revert_postgres_exporter_systemd_unit_template(
    settings: PrometheusSettings,
) -> None:
    systemd.uninstall("postgres_exporter@.service")


@task
def postgresql_backup_systemd_templates(*, env: Optional[str] = None) -> None:
    environment = ""
    if env:
        environment = f"\nEnvironment={env}\n"
    service_content = systemd.template("postgresql-backup.service").format(
        environment=environment,
        python=sys.executable,
    )
    systemd.install("postgresql-backup@.service", service_content)
    timer_content = systemd.template("postgresql-backup.timer").format(
        # TODO: use a setting for that value
        calendar="daily",
    )
    systemd.install("postgresql-backup@.timer", timer_content)


@postgresql_backup_systemd_templates.revert
def revert_postgresql_backup_systemd_templates(*, env: Optional[str] = None) -> None:
    systemd.uninstall("postgresql-backup@.service")
    systemd.uninstall("postgresql-backup@.timer")


def do(ctx: BaseContext, env: Optional[str] = None) -> None:
    settings = ctx.settings
    with runner(ctx):
        postgresql_systemd_unit_template(settings.postgresql, env=env)
        postgres_exporter_systemd_unit_template(settings.prometheus)
        postgresql_backup_systemd_templates(env=env)


def undo(ctx: BaseContext) -> None:
    settings = ctx.settings
    with runner(ctx):
        revert_postgresql_backup_systemd_templates()
        revert_postgres_exporter_systemd_unit_template(settings.prometheus)
        revert_postgresql_systemd_unit_template(settings.postgresql)
