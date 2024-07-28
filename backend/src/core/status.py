""" Store the current status of the backend and manage status changes
"""

from enum import StrEnum

from core.app_logging import getLogger, logExit

LOG = getLogger(__name__)

from core.app import App
from core.base_objects import StatusBaseClass, Status


class AppStatus(StatusBaseClass):
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


App.set_status_class(AppStatus, Status)
logExit(LOG)
