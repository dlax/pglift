from functools import partial
from typing import IO, TYPE_CHECKING

import click

from .. import prometheus, task
from ..cli import CONSOLE, Group, foreground_option, pass_component_settings, pass_ctx
from ..models import helpers

if TYPE_CHECKING:
    from ..ctx import Context
    from ..settings import PrometheusSettings

pass_prometheus_settings = partial(
    pass_component_settings, prometheus, "Prometheus postgres_exporter"
)


@click.group("postgres_exporter", cls=Group)
@pass_ctx
def postgres_exporter(ctx: "Context") -> None:
    """Handle Prometheus postgres_exporter"""


@postgres_exporter.command("schema")
def postgres_exporter_schema() -> None:
    """Print the JSON schema of database model"""
    CONSOLE.print_json(prometheus.PostgresExporter.schema_json(indent=2))


@postgres_exporter.command("apply")
@click.option("-f", "--file", type=click.File("r"), metavar="MANIFEST", required=True)
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_apply(
    ctx: "Context", settings: "PrometheusSettings", file: IO[str]
) -> None:
    """Apply manifest as a Prometheus postgres_exporter."""
    exporter = prometheus.PostgresExporter.parse_yaml(file)
    prometheus.apply(ctx, exporter, settings)


@postgres_exporter.command("install")
@helpers.parameters_from_model(prometheus.PostgresExporter)
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_install(
    ctx: "Context",
    settings: "PrometheusSettings",
    postgresexporter: prometheus.PostgresExporter,
) -> None:
    """Install the service for a (non-local) instance."""
    with task.transaction():
        prometheus.apply(ctx, postgresexporter, settings)


@postgres_exporter.command("uninstall")
@click.argument("name")
@pass_ctx
def postgres_exporter_uninstall(ctx: "Context", name: str) -> None:
    """Uninstall the service."""
    prometheus.drop(ctx, name)


@postgres_exporter.command("start")
@click.argument("name")
@foreground_option
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_start(
    ctx: "Context", settings: "PrometheusSettings", name: str, foreground: bool
) -> None:
    """Start postgres_exporter service NAME.

    The NAME argument is a local identifier for the postgres_exporter
    service. If the service is bound to a local instance, it should be
    <version>-<name>.
    """
    prometheus.start(ctx, name, settings, foreground=foreground)


@postgres_exporter.command("stop")
@click.argument("name")
@pass_prometheus_settings
@pass_ctx
def postgres_exporter_stop(
    ctx: "Context", settings: "PrometheusSettings", name: str
) -> None:
    """Stop postgres_exporter service NAME.

    The NAME argument is a local identifier for the postgres_exporter
    service. If the service is bound to a local instance, it should be
    <version>-<name>.
    """
    prometheus.stop(ctx, name, settings)
