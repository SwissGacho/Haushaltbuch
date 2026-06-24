"""Store app configuration"""

from typing import Any
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.persistent_business_object import (
    Specialized,
    Singleton,
    Personal,
    PersistentBusinessObject,
)
from business_objects.bo_descriptors import BODict, BORelation, AttributeDescription
from bom_persistent.management.user import User
from database.sql_clause import ColumnName


class Configuration(PersistentBusinessObject):
    "Persistent configuration (global or user specific)"

    configuration = BODict()
    _table = "configurations"

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


class ApplicationConfiguration(Specialized, Singleton, Configuration):
    "Persistent (non-user-specific) configuration for the whole application"

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return "Global Configuration"


class PersonalConfiguration(Specialized, Personal, Configuration):
    "Persistent configuration for a specific user"

    user_id = BORelation(User)

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return "Personal Configuration"


log_exit(LOG)
