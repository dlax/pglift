import logging
from typing import TYPE_CHECKING, Type

from pgtoolkit.conf import Configuration

from .. import hookimpl
from ..models import system
from ..settings import EXTENSIONS_CONFIG
from . import impl, models
from .impl import POWA_EXTENSIONS
from .impl import available as available

if TYPE_CHECKING:

    from ..ctx import BaseContext
    from ..models import interface

logger = logging.getLogger(__name__)


@hookimpl  # type: ignore[misc]
def instance_configuration(
    ctx: "BaseContext", manifest: "interface.Instance"
) -> Configuration:
    settings = available(ctx)
    assert settings is not None
    extensions = [e for e in POWA_EXTENSIONS if EXTENSIONS_CONFIG[e][0]]
    conf = Configuration()
    conf["shared_preload_libraries"] = ", ".join(extensions)
    return conf


@hookimpl  # type: ignore[misc]
def instance_configure(
    ctx: "BaseContext",
    manifest: "interface.Instance",
    config: Configuration,
    creating: bool,
) -> None:
    """Install PoWA for an instance when it gets configured."""
    settings = available(ctx)
    if not settings:
        logger.warning("PoWA not available, skipping configuration")
        return
    instance = system.PostgreSQLInstance.system_lookup(
        ctx, (manifest.name, manifest.version)
    )
    if creating:
        impl.setup_db(ctx, instance, manifest.powa, settings)  # type: ignore[attr-defined]


@hookimpl  # type: ignore[misc]
def interface_model() -> Type[models.ServiceManifest]:
    return models.ServiceManifest


@hookimpl  # type: ignore[misc]
def get(ctx: "BaseContext", instance: "system.Instance") -> models.ServiceManifest:
    return models.ServiceManifest()
