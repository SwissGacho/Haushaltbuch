"""Store app configuration"""

from typing import Optional

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from persistance.persistant_business_object import PersistentBusinessObject
from persistance.bo_descriptors import BODict, BORelation
from data.management.user import User


class Configuration(PersistentBusinessObject):
    "Persistant configuration (global or user specific)"

    user_id = BORelation(User)
    configuration = BODict()

    @property
    def configuration_dict(self):
        "Configuration as a dict"
        if not isinstance(self.configuration, dict):
            return {}
        return self.configuration
