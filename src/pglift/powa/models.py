from typing import Optional

import pydantic
from pydantic import Field

from .. import types


class ServiceManifest(types.ServiceManifest, service_name="powa"):
    password: Optional[pydantic.SecretStr] = Field(
        default=None,
        description="Password of PostgreSQL role for PoWA.",
        exclude=True,
    )
