"""Store app configuration"""

from typing import Optional

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BODict, BORelation
from data.management.user import User
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Configuration(BOBase):
    user_id = BORelation(flag_values={"relation": User})
    configuration = BODict()

    @property
    def configuration_dict(self):
        if not isinstance(self.configuration, dict):
            return {}
        return self.configuration
