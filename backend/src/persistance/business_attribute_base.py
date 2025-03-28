"""Base module for Business Logic on attributes of Business Objects

Describes additional data types that can exist on business objects
"""

from enum import Flag, auto
from typing import Union, Self

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)


class BaseFlag(Flag):
    @classmethod
    def flags(cls, value: str) -> Self:
        "UserRole: str(UserRole.role(value))==value"
        return cls(sum([cls[f.strip().upper()].value for f in str(value).split(",")]))

    def __str__(self) -> str:
        return ",".join([str(r.name).lower() for r in self])
