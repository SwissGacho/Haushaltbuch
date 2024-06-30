"""Store app configuration"""

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOJSONable
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Configuration(BOBase):
    configuration = BOJSONable()

    def __init__(self, id=None, cfg: any = None) -> None:
        super().__init__(id=id)
        self.config = cfg

    def __repr__(self) -> str:
        return f"<Configuration (id:{self.id}, cfg:{repr(self.config)})>"
