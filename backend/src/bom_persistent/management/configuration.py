"""Store app configuration"""

from typing import Any
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.persistent_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import BODict, BORelation, AttributeDescription
from bom_persistent.management.user import User
from core.const import SINGLE_USER_NAME


class Configuration(PersistentBusinessObject):
    "Persistent configuration (global or user specific)"

    user_id = BORelation(User)
    configuration = BODict()

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        if self.user_id is None:
            return "Global Configuration"
        if not isinstance(self.user_id, User):
            LOG.warning(
                f"Configuration.display_name: user_id is not a User instance: {self.user_id}"
            )
            return "Invalid User Configuration"
        if self.user_id.name == SINGLE_USER_NAME:
            return "Single User Configuration"
        return f"Configuration for user ({self.user_id.display_name})"

    @classmethod
    def display_name_components(cls) -> list[str]:
        """Return a list of attribute names that should be used to construct the display name."""
        return super().display_name_components() + ["user_id"]

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
