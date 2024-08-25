""" User business object """

from typing import Optional, Union
from enum import Flag, auto

from core.app_logging import getLogger, log_exit
from persistance.bo_descriptors import BOStr

LOG = getLogger(__name__)
from typing import Self

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOStr


class UserRole(Flag):
    "User Roles/Permissions"
    ADMIN = auto()
    USER = auto()

    def __str__(self) -> str:
        return ",".join([str(r.name).lower() for r in self])

    @classmethod
    def role(cls, value: Union[str, BOStr]) -> Self:
        "UserRole: str(UserRole.role(value))==value"
        return cls(sum([cls[f.strip().upper()].value for f in str(value).split(",")]))


class User(BOBase):
    "Persistant user object"
    name = BOStr()
    password = BOStr()
    role = BOStr()


log_exit(LOG)
