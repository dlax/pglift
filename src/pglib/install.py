import sys
from typing import Optional

from . import systemd
from .settings import PostgreSQLSettings, PrometheusSettings, Settings
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
    configpath = settings.configpath.replace("{instance.version}-{instance.name}", "%i")
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


def do(settings: Settings, env: Optional[str] = None) -> None:
    with runner():
        postgresql_systemd_unit_template(settings.postgresql, env=env)
        postgres_exporter_systemd_unit_template(settings.prometheus)


def undo(settings: Settings) -> None:
    with runner():
        revert_postgres_exporter_systemd_unit_template(settings.prometheus)
        revert_postgresql_systemd_unit_template(settings.postgresql)


if __name__ == "__main__":  # pragma: nocover
    import argparse

    from .settings import SETTINGS

    parser = argparse.ArgumentParser(
        description="Manage installation of extra data files"
    )
    parser.add_argument(
        "--settings",
        help="custom settings file",
    )
    parser.add_argument(
        "--uninstall",
        action="store_true",
        default=False,
        help="perform an uninstallation",
    )
    args = parser.parse_args()

    if args.uninstall:
        undo(SETTINGS)
    else:
        env = None
        if args.settings:
            env = f"SETTINGS=@{args.settings}"
        do(SETTINGS, env=env)
