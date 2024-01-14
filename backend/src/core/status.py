""" Store the current status of the backend and manage status changes
"""

STATUS_UNDEF = "undefined"
STATUS_UNCONFIGURED = "unconfigured"
STATUS_NO_DB = "noDB"
STATUS_CHECK_DB = "checkingDBschema"
STATUS_OLD_DB = "outdatedDBschema"


class _Status:
    def __init__(self) -> None:
        self._status = STATUS_UNDEF

    @property
    def status(self):
        "Current status of the app"
        return self._status

    @status.setter
    def status(self, value=None):
        if value is not None:
            self._status = value


app = _Status()
