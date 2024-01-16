""" Store the current status of the backend and manage status changes
"""

from enum import Enum
from core.base_object import BaseObject


class Status(Enum):
    STATUS_UNDEF = "undefined"
    STATUS_UNCONFIGURED = "unconfigured"
    STATUS_NO_DB = "noDB"
    STATUS_CHECK_DB = "checkingDBschema"
    STATUS_OLD_DB = "outdatedDBschema"


class _Status(BaseObject):
    def __init__(self) -> None:
        self._status = Status.STATUS_UNDEF

    @property
    def status(self):
        "Current status of the app"
        return self._status

    @status.setter
    def status(self, value=None):
        if value is not None:
            self._status = Status(value)

    def __str__(self) -> str:
        return self._status.value

    def __repr__(self) -> str:
        return f"<Status({self._status.value})>"


app = _Status()
