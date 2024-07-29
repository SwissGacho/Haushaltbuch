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

    def __init__(
        self, id=None, name: str = None, pw: str = None, role: UserRole = None
    ) -> None:
        super().__init__(id=id)
        self.name = name
        self.password = pw
        self.role = role

    def __repr__(self) -> str:
        return f"<User id:{self.id}, name:{self.name}, role:{self.role}>"


# LOG.debug(f"{BO_Base._business_objects=}")
