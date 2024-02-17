""" Store the current status of the backend and manage status changes
"""

from enum import StrEnum
from core.base_object import BaseObject
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Status(StrEnum):
    STATUS_UNCONFIGURED = "unconfigured"
    STATUS_NO_DB = "noDB"
    STATUS_DB_CFG = "DBconfigured"
    STATUS_DB_UNSUPPORTED = "DBunsuppoerted"
    STATUS_CHECK_DB = "checkingDBschema"
    STATUS_OLD_DB = "outdatedDBschema"


class AppStatus(BaseObject):
    def __init__(self) -> None:
        self._status = Status.STATUS_UNCONFIGURED

    @property
    def status(self):
        "Current status of the app"
        return self._status

    @status.setter
    def status(self, value=None):
        if value is not None:
            self._status = Status(value)


# LOG.debug("module imported")
