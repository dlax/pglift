import logging
from typing import TYPE_CHECKING, Optional, Type

from .. import exceptions, hookimpl, systemd, util
from . import impl, models
from .impl import apply as apply
from .impl import available as available
from .impl import start as start
from .impl import stop as stop
from .models import PostgresExporter as PostgresExporter

if TYPE_CHECKING:
    import click
    from pgtoolkit.conf import Configuration

    from ..ctx import BaseContext
    from ..models import interface, system

__all__ = ["PostgresExporter", "apply", "available", "start", "stop"]

logger = logging.getLogger(__name__)


@hookimpl  # type: ignore[misc]
def system_lookup(
    ctx: "BaseContext", instance: "system.BaseInstance"
) -> Optional[models.Service]:
    settings = available(ctx)
    assert settings is not None
    try:
        p = impl.port(instance.qualname, settings)
    except exceptions.FileNotFoundError as exc:
        logger.debug("failed to read postgres_exporter port for %s: %s", instance, exc)
        return None
    else:
        return models.Service(port=p)


@hookimpl  # type: ignore[misc]
def interface_model() -> Type[models.ServiceManifest]:
    return models.ServiceManifest


@hookimpl  # type: ignore[misc]
def describe(
    ctx: "BaseContext", instance: "system.Instance"
) -> Optional[models.ServiceManifest]:
    try:
        s = instance.service(models.Service)
    except ValueError:
        return None
    else:
        return models.ServiceManifest(port=s.port)


SYSTEMD_SERVICE_NAME = "pglift-postgres_exporter@.service"


@hookimpl  # type: ignore[misc]
def install_systemd_unit_template(ctx: "BaseContext", header: str = "") -> None:
    logger.info("installing systemd template unit for Prometheus postgres_exporter")
    settings = available(ctx)
    assert settings is not None
    configpath = str(settings.configpath).replace("{name}", "%i")
    content = systemd.template(SYSTEMD_SERVICE_NAME).format(
        executeas=systemd.executeas(ctx.settings),
        configpath=configpath,
        execpath=settings.execpath,
    )
    systemd.install(
        SYSTEMD_SERVICE_NAME,
        util.with_header(content, header),
        ctx.settings.systemd.unit_path,
        logger=logger,
    )


@hookimpl  # type: ignore[misc]
def uninstall_systemd_unit_template(ctx: "BaseContext") -> None:
    logger.info("uninstalling systemd template unit for Prometheus postgres_exporter")
    systemd.uninstall(
        SYSTEMD_SERVICE_NAME, ctx.settings.systemd.unit_path, logger=logger
    )


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: "BaseContext", manifest: "interface.Instance", config: "Configuration"
) -> None:
    """Install postgres_exporter for an instance when it gets configured."""
    settings = available(ctx)
    if not settings:
        logger.warning(
            "Prometheus postgres_exporter not available, skipping monitoring configuration"
        )
        return
    impl.setup_local(ctx, manifest, settings, config)


@hookimpl  # type: ignore[misc]
def instance_start(ctx: "BaseContext", instance: "system.Instance") -> None:
    """Start postgres_exporter service."""
    settings = available(ctx)
    if not settings or not impl.enabled(instance.qualname, settings):
        return
    impl.start(ctx, instance.qualname, settings)


@hookimpl  # type: ignore[misc]
def instance_stop(ctx: "BaseContext", instance: "system.Instance") -> None:
    """Stop postgres_exporter service."""
    settings = available(ctx)
    if not settings or not impl.enabled(instance.qualname, settings):
        return
    impl.stop(ctx, instance.qualname, settings)


@hookimpl  # type: ignore[misc]
def instance_drop(ctx: "BaseContext", instance: "system.Instance") -> None:
    """Uninstall postgres_exporter from an instance being dropped."""
    settings = available(ctx)
    if not settings:
        return
    impl.revert_setup(ctx, instance.qualname, settings)


@hookimpl  # type: ignore[misc]
def cli() -> "click.Group":
    from .cli import postgres_exporter

    return postgres_exporter
