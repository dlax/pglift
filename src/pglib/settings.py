import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar

from pydantic import BaseSettings, Field
from pydantic.env_settings import SettingsSourceCallable
from typing_extensions import Literal

T = TypeVar("T", bound=BaseSettings)


def frozen(cls: Type[T]) -> Type[T]:
    cls.Config.frozen = True
    return cls


@frozen
class PostgreSQLSettings(BaseSettings):
    """Settings for PostgreSQL."""

    versions: List[str] = ["13", "12", "11", "10", "9.6"]
    """Available PostgreSQL versions."""

    root: Path = Path("/var/lib/pgsql")
    """Root directory for all managed instances."""

    locale: Optional[str] = "C"
    """Instance locale as used by initdb."""

    surole: str = "postgres"
    """User name of instance super-user."""

    instancedir: str = "{version}/{instance}"
    """Path segment to instance base directory relative to `root` path."""

    datadir: str = "data"
    """Path segment from instance base directory to PGDATA directory."""

    waldir: str = "wal"
    """Path segment from instance base directory to WAL directory."""

    pid_directory: Path = Path("/run/postgresql")
    """Path to directory where postgres process PID file will be written."""

    class Config:
        env_prefix = "pglib_postgresql_"


@frozen
class PgBackRestSettings(BaseSettings):
    """Settings for pgBackRest."""

    execpath: str = "/usr/bin/pgbackrest"
    """Path to the pbBackRest executable."""

    configpath: str = (
        "/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf"
    )
    """Path to the config file."""

    directory: str = "/var/lib/pgbackrest/{instance.version}-{instance.name}"
    """Path to the directory where backups are stored."""

    logpath: str = "/var/lib/pgbackrest/{instance.version}-{instance.name}/logs"
    """Path where log files are stored."""

    class Config:
        env_prefix = "pglib_pgbackrest_"


@frozen
class PrometheusSettings(BaseSettings):
    """Settings for Prometheus postgres_exporter"""

    execpath: str = "/usr/bin/prometheus-postgres-exporter"
    """Path to the postgres_exporter executable."""

    configpath: str = (
        "/etc/prometheus/postgres_exporter-{instance.version}-{instance.name}.conf"
    )
    """Path to the config file."""

    queriespath: str = "/etc/prometheus/postgres_exporter_queries-{instance.version}-{instance.name}.yaml"
    """Path to the queries file."""

    class Config:
        env_prefix = "pglib_prometheus_"


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """Load settings values from 'SETTINGS' environment variable.

    If this variable has a value starting with @, it is interpreted as a path
    to a JSON file. Otherwise, a JSON serialization is expected.
    """
    env_settings = os.getenv("SETTINGS")
    if not env_settings:
        return {}
    if env_settings.startswith("@"):
        config = Path(env_settings[1:])
        encoding = settings.__config__.env_file_encoding
        # May raise FileNotFoundError, which is okay here.
        env_settings = config.read_text(encoding)
    return json.loads(env_settings)  # type: ignore[no-any-return]


def maybe_systemd() -> Optional[Literal["systemd"]]:
    if shutil.which("systemctl") is not None:
        return "systemd"
    return None


@frozen
class Settings(BaseSettings):

    postgresql: PostgreSQLSettings = PostgreSQLSettings()
    pgbackrest: PgBackRestSettings = PgBackRestSettings()
    prometheus: PrometheusSettings = PrometheusSettings()

    service_manager: Optional[Literal["systemd"]] = Field(default_factory=maybe_systemd)
    scheduler: Optional[Literal["systemd"]] = Field(default_factory=maybe_systemd)

    class Config:
        env_prefix = "pglib_"

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            return (
                init_settings,
                json_config_settings_source,
                env_settings,
                file_secret_settings,
            )


SETTINGS = Settings()


if __name__ == "__main__":

    print(SETTINGS.json(indent=2))
