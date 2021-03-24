from pathlib import Path
from typing import List, Optional, Type, TypeVar

from pydantic import BaseSettings

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

    class Config:
        env_prefix = "postgresql_"


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
        env_prefix = "pgbackrest_"


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
        env_prefix = "prometheus_"


@frozen
class Settings(BaseSettings):

    postgresql: PostgreSQLSettings = PostgreSQLSettings()
    pgbackrest: PgBackRestSettings = PgBackRestSettings()
    prometheus: PrometheusSettings = PrometheusSettings()

    class Config:
        env_prefix = "pglib_"


SETTINGS = Settings()


if __name__ == "__main__":

    print(SETTINGS.json(indent=2))
