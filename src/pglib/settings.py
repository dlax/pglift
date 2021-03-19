import functools
import json
from pathlib import Path
from typing import Any, List, Optional

import attr
import environ
from attr.validators import instance_of


@environ.config(frozen=True)
class PostgreSQLSettings:
    """Settings for PostgreSQL."""

    versions: List[str] = environ.var(
        default=attr.Factory(lambda: ["13", "12", "11", "10", "9.6"])
    )
    """Available PostgreSQL versions."""

    root: Path = environ.var(
        default=Path("/var/lib/pgsql"), validator=instance_of(Path), converter=Path
    )
    """Root directory for all managed instances."""

    locale: Optional[str] = environ.var(default="C")
    """Instance locale as used by initdb."""

    surole: str = environ.var(default="postgres")
    """User name of instance super-user."""

    instancedir: str = environ.var(default="{version}/{instance}")
    """Path segment to instance base directory relative to `root` path."""

    datadir: str = environ.var(default="data")
    """Path segment from instance base directory to PGDATA directory."""

    waldir: str = environ.var("wal")
    """Path segment from instance base directory to WAL directory."""


@environ.config(frozen=True)
class PgBackRestSettings:
    """Settings for pgBackRest."""

    execpath: str = environ.var(default="/usr/bin/pgbackrest")
    """Path to the pbBackRest executable."""

    configpath: str = environ.var(
        default="/etc/pgbackrest/pgbackrest-{instance.version}-{instance.name}.conf"
    )
    """Path to the config file."""

    directory: str = environ.var(
        default="/var/lib/pgbackrest/{instance.version}-{instance.name}"
    )
    """Path to the directory where backups are stored."""

    logpath: str = environ.var(
        default="/var/lib/pgbackrest/{instance.version}-{instance.name}/logs"
    )
    """Path where log files are stored."""


@environ.config(frozen=True)
class PrometheusSettings:
    """Settings for Prometheus postgres_exporter"""

    execpath: str = environ.var(default="/usr/bin/prometheus-postgres-exporter")
    """Path to the postgres_exporter executable."""

    configpath: str = environ.var(
        default="/etc/prometheus/postgres_exporter-{instance.version}-{instance.name}.conf"
    )
    """Path to the config file."""

    queriespath: str = environ.var(
        default="/etc/prometheus/postgres_exporter_queries-{instance.version}-{instance.name}.yaml",
    )
    """Path to the queries file."""


@environ.config(prefix="PGLIB", frozen=True)
class Settings:

    postgresql: PostgreSQLSettings = environ.group(PostgreSQLSettings)
    pgbackrest: PgBackRestSettings = environ.group(PgBackRestSettings)
    prometheus: PrometheusSettings = environ.group(PrometheusSettings)


to_config = functools.partial(environ.to_config, Settings)

SETTINGS = to_config()


if __name__ == "__main__":

    def default(obj: Any) -> Any:
        if isinstance(obj, Path):
            return str(obj)
        return obj

    print(json.dumps(attr.asdict(SETTINGS), indent=2, default=default))
