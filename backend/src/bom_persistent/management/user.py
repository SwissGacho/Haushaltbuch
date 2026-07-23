"""User business object"""

from enum import auto

from business_objects.bo_semantic_role import BOSemanticRole
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.business_attribute_base import BaseFlag
from business_objects.persistent_business_object import (
    PersistentBusinessObject,
    Specialized,
    Singleton,
)
from business_objects.bo_descriptors import AttributeDescription, BOStr, BOFlag


class UserRole(BaseFlag):
    "User Roles/Permissions"

    ADMIN = auto()
    USER = auto()


class GenericUser(PersistentBusinessObject):
    "Generic user object (needs specialization)"

    role = BOFlag(UserRole)
    _table = "users"

    @classmethod
    def navigation_header(
        cls, ref: AttributeDescription | str | None = None
    ) -> dict[str, str] | None:
        return {"name": "genericuser", "display_name": "User"}

    @property
    def is_admin(self) -> bool:
        """Check if the user has admin role"""
        return UserRole.ADMIN in self.role


class SingleUser(Specialized, Singleton, GenericUser):
    "Persistent user object for single-user mode"

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return "Single-User"


class User(Specialized, GenericUser):
    "Persistent user object"

    name = BOStr(semantic_role=BOSemanticRole.BONAME)
    password = BOStr()

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return self.name


log_exit(LOG)
