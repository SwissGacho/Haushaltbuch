""" Common constants
"""
from core.app_logging import getLogger
from core.config import AppConfiguration, Config
from core.status import AppStatus, Status

LOG = getLogger(__name__)
WEBSOCKET_PORT = 8765


class App:
    "keep status and configuration of the app"

    def __init__(self) -> None:
        self._status = None
        self._config = None

    def initialize(self):
        self._status = AppStatus()
        self._config = AppConfiguration()
        self._status.status = (
            Status.STATUS_DB_CFG
            if self._config.configuration.get(Config.CONFIG_DB.value, {}).get(
                Config.CONFIG_DB_DB.value
            )
            else Status.STATUS_NO_DB
        )
        LOG.debug("app initialized")

    @property
    def status(self):
        return self._status.status

    @property
    def configuration(self):
        return self._config.configuration


app = App()
# LOG.debug("module imported")
