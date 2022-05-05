from pathlib import Path
from typing import Optional

from pglift import util
from pglift.ctx import Context


class NoSiteContext(Context):
    @staticmethod
    def site_config(*parts: str) -> Optional[Path]:
        config = util.dist_config(*parts)
        if config:
            return config
        return None
