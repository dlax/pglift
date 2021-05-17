from attr.validators import in_

from .settings import SETTINGS

known_postgresql_version = in_(SETTINGS.postgresql.versions)
