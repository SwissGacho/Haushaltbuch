""" User business object """

from typing import Optional
from enum import StrEnum

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOStr


class UserRole(StrEnum):
    "User Roles/Permissions"
    ROLE_ADMIN = "admin"
    ROLE_USER = "user"


class User(BOBase):
    "Persistant user object"
    name = BOStr()
    password = BOStr()
    role = BOStr()

    def __init__(
        self,
        id=None,
        name: Optional[str] = None,
        pw: Optional[str] = None,
        role: Optional[UserRole] = None,
    ) -> None:
        super().__init__(id=id)
        self.name = name
        self.password = pw
        self.role = role

    def __repr__(self) -> str:
        return f"<User id:{self.id}, name:{self.name}, role:{self.role}>"


log_exit(LOG)
