""" Common constants
"""

from core.app_logging import getLogger
from core.config import AppConfiguration, Config
from core.status import AppStatus, Status

# from db.db_base import DB

LOG = getLogger(__name__)
WEBSOCKET_PORT = 8765


class _classproperty(property):
    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


class App:
    "keep status and configuration of the app"

    _app = None
    _status = None
    _config = None
    _db = None

    @classmethod
    def initialize(cls, app_location: str):
        "Initialize global objects (Status, Config)"
        cls._status = AppStatus()
        cls._config = AppConfiguration(app_location)
        cls._status.status = (
            Status.STATUS_DB_CFG
            if cls._config.configuration().get(Config.CONFIG_DB, {})
            else Status.STATUS_NO_DB
        )
        # LOG.debug(f"App: {str(Config.CONFIG_DB)=}   {cls._config.configuration()=}")
        LOG.debug("app initialized")

    @_classproperty
    def status(self) -> AppStatus:
        "Global status of the app"
        return self._status.status

    @_classproperty
    def config_object(self):
        """The app's configuration object.
        This should only be used for calling config methods.
        """
        return self._config

    @_classproperty
    def configuration(self) -> dict:
        "Global configuration of the app"
        return self._config.configuration()

    @_classproperty
    def db(self) -> "DB":
        "Global DB object"
        return self._db

    @classmethod
    def set_db(cls, db: "DB"):
        "Set the global DB object"
        cls._db = db


# LOG.debug("module imported")
