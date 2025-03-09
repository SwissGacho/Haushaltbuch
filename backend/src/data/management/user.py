""" User business object """

from typing import Optional, Union, Self
from enum import Flag, auto

from core.app_logging import getLogger, log_exit
from persistance.bo_descriptors import BOStr

LOG = getLogger(__name__)

from persistance.business_attribute_base import BaseFlag
from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOStr, BOFlag


class UserRole(BaseFlag):
    "User Roles/Permissions"
    ADMIN = auto()
    USER = auto()


class User(BOBase):
    "Persistant user object"
    name = BOStr()
    password = BOStr()
    role = BOFlag(UserRole)


log_exit(LOG)
