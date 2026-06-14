"""Store app configuration"""

from typing import Any
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.persistent_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import BODict, BORelation, AttributeDescription
from bom_persistent.management.user import User


class Configuration(PersistentBusinessObject):
    "Persistent configuration (global or user specific)"

    user_id = BORelation(User)
    configuration = BODict()

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        if self.user_id is None:
            return "Global Configuration"
        return f"Configuration for user ({self.user_id})"

    @classmethod
    def navigation_header(
        cls, ref: AttributeDescription | str | None = None
    ) -> dict[str, str] | None:
        """This BO should not be rendered for navigation directly.
        Editing is handled by the TransientBusinessObject EditConfig"""
        return None

    @property
    def configuration_dict(self) -> dict[str, Any]:
        "Configuration as a dict"
        if not isinstance(self.configuration, dict):
            return {}
        return self.configuration


log_exit(LOG)
