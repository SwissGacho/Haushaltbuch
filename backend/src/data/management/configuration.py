"""Store app configuration"""

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BODict, BORelation
from data.management.user import User
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Configuration(BOBase):
    user_id = BORelation(User)
    configuration = BODict()

    def __init__(self, id: int = None, cfg: any = None, user_id: int = None) -> None:
        super().__init__(id=id)
        self.user_id = user_id
        self.configuration = cfg

    def __repr__(self) -> str:
        return f"<Configuration (id:{self.id}, user_id:{self.user_id}, cfg:'{repr(self.configuration)}')>"
