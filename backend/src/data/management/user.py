""" User business object """

from enum import StrEnum

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOStr

from core.app_logging import getLogger

LOG = getLogger(__name__)


class UserRole(StrEnum):
    ROLE_ADMIN = "admin"
    ROLE_USER = "user"


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
