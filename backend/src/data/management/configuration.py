"""Store app configuration"""

from typing import Optional

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BODict, BORelation
from data.management.user import User


class Configuration(BOBase):
    "Persistant configuration (global or user specific)"
    user_id = BORelation(User)
    configuration = BODict()

    def __init__(
        self,
        id: Optional[int] = None,
        cfg: Optional[dict] = None,
        user_id: Optional[int] = None,
    ) -> None:
        super().__init__(id=id)
        self.user_id = user_id
        self.configuration = cfg

    @property
    def configuration_dict(self):
        "Configuration as a dict"
        if not isinstance(self.configuration, dict):
            return {}
        return self.configuration

    def __repr__(self) -> str:
        return (
            f"<Configuration (id:{self.id},"
            f" user_id:{self.user_id},"
            f" cfg:'{repr(self.configuration)}')>"
        )


log_exit(LOG)
