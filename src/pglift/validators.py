from attr.validators import in_

from .settings import POSTGRESQL_SUPPORTED_VERSIONS

known_postgresql_version = in_(POSTGRESQL_SUPPORTED_VERSIONS)
