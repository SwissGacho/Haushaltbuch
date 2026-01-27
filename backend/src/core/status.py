"""Store the current status of the backend and manage status changes"""

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.app import App
from core.base_objects import StatusBaseClass, Status, VersionInfo


class AppStatus(StatusBaseClass):
    "Global status object (instantiated only once)"

    def __init__(self, version: str | None = None) -> None:
        self._status = Status.STATUS_UNCONFIGURED
        self._version_info: dict[VersionInfo, str] = {
            VersionInfo.VERSION: version or "development"
        }

    @property
    def status(self):
        "Current status of the app"
        return self._status

    @status.setter
    def status(self, value=None):
        if value is not None:
            self._status = Status(value)

    @property
    def version(self) -> dict[VersionInfo, str]:
        "Current version of the app"
        return self._version_info


App.set_status_class(AppStatus, Status)

log_exit(LOG)
