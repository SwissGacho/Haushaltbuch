"""Base module for Business Logic on attributes of Business Objects

Describes additional data types that can exist on business objects
"""

from enum import Flag
from typing import Self

from core.app_logging import getLogger

LOG = getLogger(__name__)


class BaseFlag(Flag):
    "Base class for Flags used in Business Objects"

    @classmethod
    def flags(cls, value: str) -> Self:
        """Convert a string listing flag values into a Flag instance,
        e.g. UserRole: str(UserRole.role(value))==value"""
        try:
            return cls(
                sum([cls[f.strip().upper()].value for f in str(value).split(",")])
            )
        except KeyError as exc:
            raise ValueError(f"Invalid flag in '{value}' for {cls.__name__}") from exc

    def __str__(self) -> str:
        return ",".join([str(r.name).lower() for r in self])

    def json_encode(self) -> str:
        "return JSONable representation of the flag"
        return str(self)


log_exit(LOG)
