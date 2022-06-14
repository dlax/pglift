from typing import Optional

from pydantic import Field, SecretStr

from .. import types


class ServiceManifest(types.ServiceManifest, service_name="pgbackrest"):

    password: Optional[SecretStr] = Field(
        default=None,
        description="Password of PostgreSQL role for pgBackRest.",
        exclude=True,
    )
