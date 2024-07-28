"""Store app configuration"""

from typing import Optional

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BODict, BORelation
from data.management.user import User
from core.base_objects import ConfigDict
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Configuration(BOBase):
    user_id = BORelation(User)
    configuration = BODict()

    def __init__(
        self,
        id: Optional[int] = None,
        cfg: Optional[ConfigDict] = None,
        user_id: Optional[int] = None,
    ) -> None:
        super().__init__(id=id)
        self.user_id = user_id
        self.configuration = cfg

    @property
    def configuration_dict(self):
        if not isinstance(self.configuration, dict):
            return {}
        return self.configuration

    def __repr__(self) -> str:
        return f"<Configuration (id:{self.id}, user_id:{self.user_id}, cfg:'{repr(self.configuration)}')>"
