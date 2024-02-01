""" Common constants
"""
from core.app_logging import getLogger
from core.config import AppConfiguration, Config
from core.status import AppStatus, Status

LOG = getLogger(__name__)
WEBSOCKET_PORT = 8765


class App:
    "keep status and configuration of the app"

    _status = None
    _config = None

    @classmethod
    def initialize(cls):
        cls._status = AppStatus()
        cls._config = AppConfiguration()
        cls._status.status = (
            Status.STATUS_DB_CFG
            if cls._config.configuration.get(Config.CONFIG_DB, {}).get(
                Config.CONFIG_DB_DB
            )
            else Status.STATUS_NO_DB
        )
        LOG.debug("app initialized")

    @classmethod
    @property
    def status(cls):
        return cls._status.status

    @classmethod
    @property
    def configuration(cls):
        return cls._config.configuration


# LOG.debug("module imported")
