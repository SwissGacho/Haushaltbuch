""" Store the current status of the backend and manage status changes
"""

from core.app import App
from core.base_objects import StatusBaseClass, Status
from core.app_logging import getLogger

LOG = getLogger(__name__)


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
# LOG.debug("module imported")
