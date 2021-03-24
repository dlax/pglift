import textwrap
from pathlib import Path

from .ctx import BaseContext
from .model import Instance
from .settings import SETTINGS, PrometheusSettings
from .task import task

PROMETHEUS_SETTINGS = SETTINGS.prometheus


def _configpath(instance: Instance, settings: PrometheusSettings) -> Path:
    return Path(settings.configpath.format(instance=instance))


def _queriespath(instance: Instance, settings: PrometheusSettings) -> Path:
    return Path(settings.queriespath.format(instance=instance))


@task
def setup(
    ctx: BaseContext,
    instance: Instance,
    *,
    settings: PrometheusSettings = PROMETHEUS_SETTINGS,
) -> None:
    """Setup postgres_exporter for Prometheus"""
    configpath = _configpath(instance, settings)
    content = """
    DATA_SOURCE_URI=localhost:{instance_port}
    DATA_SOURCE_USER={role}
    PG_EXPORTER_WEB_LISTEN_ADDRESS=:{port}
    PG_EXPORTER_AUTO_DISCOVER_DATABASES=true
    PG_EXPORTER_EXTEND_QUERY_PATH={queriespath}
    POSTGRES_EXPORTER_OPTS='--log.level=info --log.format=logger:syslog?appname=postgres_exporter-{instance.version}-{instance.name}&local=0'
    """
    configpath.parent.mkdir(mode=0o750, exist_ok=True, parents=True)
    instance_config = instance.config()
    assert instance_config and "port" in instance_config
    instance_port = instance_config.port

    queriespath = _queriespath(instance, settings)
    config = {
        "instance": instance,
        "instance_port": instance_port,
        "role": "postgres",
        "port": 9187,
        "queriespath": queriespath,
    }

    if not configpath.exists():
        with configpath.open("w") as configfile:
            configfile.write(textwrap.dedent(content.format(**config)))
        configpath.chmod(0o416)

    if not queriespath.exists():
        queriespath.touch()


@setup.revert
def revert_setup(
    ctx: BaseContext,
    instance: Instance,
    *,
    settings: PrometheusSettings = PROMETHEUS_SETTINGS,
) -> None:
    """Un-setup postgres_exporter for Prometheus"""
    configpath = _configpath(instance, settings)

    if configpath.exists():
        configpath.unlink()

    queriespath = _queriespath(instance, settings)
    if queriespath.exists():
        queriespath.unlink()
