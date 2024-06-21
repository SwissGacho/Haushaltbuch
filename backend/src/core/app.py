""" Common constants
"""

from core.app_logging import getLogger
from core.config import AppConfiguration, Config
from core.status import AppStatus, Status

# from db.db_base import DB

LOG = getLogger(__name__)
WEBSOCKET_PORT = 8765


class App:
    "keep status and configuration of the app"

    _status = None
    _config = None
    _db = None

    @classmethod
    def initialize(cls, app_location: str):
        cls._status = AppStatus()
        cls._config = AppConfiguration(app_location)
        cls._status.status = (
            Status.STATUS_DB_CFG
            if cls._config.configuration.get(Config.CONFIG_DB, {})
            else Status.STATUS_NO_DB
        )
        LOG.debug("app initialized")

    @classmethod
    @property
    def status(cls) -> AppStatus:
        return cls._status.status

    @classmethod
    @property
    def configuration(cls) -> AppConfiguration:
        return cls._config.configuration

    @classmethod
    @property
    def db(cls) -> "DB":
        return cls._db

    @classmethod
    def set_db(cls, db: "DB"):
        cls._db = db


# LOG.debug("module imported")
