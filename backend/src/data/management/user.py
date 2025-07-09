"""User business object"""

from enum import auto

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from persistance.business_attribute_base import BaseFlag
from persistance.persistant_business_object import PersistentBusinessObject
from persistance.bo_descriptors import BOStr, BOFlag


class UserRole(BaseFlag):
    "User Roles/Permissions"

    ADMIN = auto()
    USER = auto()


class User(PersistentBusinessObject):
    "Persistant user object"

    name = BOStr()
    password = BOStr()
    role = BOFlag(UserRole)


log_exit(LOG)
