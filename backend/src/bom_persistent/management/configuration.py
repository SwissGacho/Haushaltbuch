"""Store app configuration"""

from typing import Any, Optional
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.persistent_business_object import (
    Specialized,
    Singleton,
    Personal,
    AdminOnly,
    PersistentBusinessObject,
)
from business_objects.bo_descriptors import BODict, BORelation, AttributeDescription
from bom_persistent.management.user import User
from database.sql_clause import ColumnName
from server.ws_connection_base import SessionBase


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


class ApplicationConfiguration(Specialized, Singleton, AdminOnly, Configuration):
    "Persistent (non-user-specific) configuration for the whole application"

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return "Global Configuration"


class PersonalConfiguration(Specialized, Personal, Configuration):
    "Persistent configuration for a specific user"

    user_id = BORelation(User)

    async def business_values_as_dict(
        self, session: Optional[SessionBase] = None
    ) -> dict[str, Any]:
        """Return the business values as a dictionary, including the user_id.
        If self.id is None, it will attempt to fetch the configuration for the current user.
        """
        LOG.debug(
            f"{str(self)}.business_values_as_dict: {self.id=}, {session=}, {getattr(session, 'user', None)=}"
        )
        if self.id is None:
            user = getattr(session, "user", None)
            # Fetch the configuration for the current user
            if user is None:
                raise ValueError(
                    "Cannot fetch personal configuration without a valid session and user"
                )
            ids = await self.get_matching_ids(session=session)
            if len(ids) == 0:
                LOG.debug(
                    f"No personal configuration found for user {getattr(session, 'user', None)}. Creating a new one."
                )
                self.user_id = user
                self.configuration = {"new": user.name}
                return await super(
                    PersistentBusinessObject, self
                ).business_values_as_dict(session=session)
            elif len(ids) > 1:
                raise ValueError(
                    f"Multiple personal configurations found for user {getattr(session, 'user', 'None')}"
                )
            self.id = ids[0]
        return await super().business_values_as_dict(session=session)

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return "Personal Configuration"


log_exit(LOG)
