""" User business object """

from enum import Flag, auto

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOStr

from core.app_logging import getLogger

LOG = getLogger(__name__)


class UserRole(Flag):
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
    name = BOStr()
    password = BOStr()
    role = BOStr()


# LOG.debug(f"{BO_Base._business_objects=}")
