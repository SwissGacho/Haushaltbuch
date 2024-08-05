""" User business object """

from enum import Flag, auto
from typing import Self

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOStr

from core.app_logging import getLogger

LOG = getLogger(__name__)


class UserRole(Flag):
    ADMIN = auto()
    USER = auto()

    def __str__(self) -> str:
        return ",".join([str(r.name).lower() for r in self])

    @classmethod
    def role(cls, value: str) -> Self:
        "UserRole: str(UserRole.role(value))==value"
        return cls(sum([cls[f.strip().upper()].value for f in value.split(",")]))


class User(BOBase):
    name = BOStr()
    password = BOStr()
    role = BOStr()


# LOG.debug(f"{BO_Base._business_objects=}")
