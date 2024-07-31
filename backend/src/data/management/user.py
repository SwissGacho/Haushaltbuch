""" User business object """

from typing import Optional
from enum import Flag, auto

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOStr


class UserRole(Flag):
    "User Roles/Permissions"
    ROLE_ADMIN = auto()
    ROLE_USER = auto()

    def __str__(self) -> str:
        strng: list[str] = []
        if UserRole.ROLE_ADMIN in self:
            strng.append("admin")
        if UserRole.ROLE_USER in self:
            strng.append("user")
        return ",".join(strng)


class User(BOBase):
    "Persistant user object"
    name = BOStr()
    password = BOStr()
    role = BOStr()


log_exit(LOG)
