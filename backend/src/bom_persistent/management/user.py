"""User business object"""

from enum import auto

from business_objects.bo_semantic_role import BOSemanticRole
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.business_attribute_base import BaseFlag
from business_objects.persistent_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import BOStr, BOFlag


class UserRole(BaseFlag):
    "User Roles/Permissions"

    ADMIN = auto()
    USER = auto()


class User(PersistentBusinessObject):
    "Persistent user object"

    name = BOStr(semantic_role=BOSemanticRole.BONAME)
    password = BOStr()
    role = BOFlag(UserRole)


log_exit(LOG)
