""" Common constants
"""

from asyncio import Event
from enum import StrEnum
from typing import ClassVar, TypeAlias

from core.app_logging import getLogger, logExit

LOG = getLogger(__name__)

# pylint: disable=wrong-import-position
import core


class _classproperty(property):
    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


GlobalStatus: TypeAlias = "core.status.AppStatus"
StatusEnum: TypeAlias = StrEnum
GlobalConfiguration: TypeAlias = "core.status.Configuration"
ConfigEnum: TypeAlias = StrEnum


class App:
    "keep status and configuration of the app"

    _status_class: ClassVar[GlobalStatus] = None
    _status_enum_class: ClassVar[StatusEnum] = None
    _config_class: ClassVar[GlobalConfiguration] = None
    _config_enum_class: ClassVar[ConfigEnum] = None
    db_available: ClassVar = Event()
    db_failure: ClassVar = Event()
    db_restart: ClassVar = Event()
    db_request_restart: ClassVar = Event()

    _app: ClassVar = None
    _status: ClassVar = None
    _config: ClassVar = None
    _db: ClassVar = None

    @classmethod
    def initialize(cls, app_location: str):
        "Initialize global objects (Status, Config)"
        if not (
            cls._status_class
            and cls._status_enum_class
            and cls._config_class
            and cls._config_enum_class
        ):
            raise ReferenceError("Status and Configuration not initialized")
        if not callable(cls._status_class):
            raise TypeError("Status class not callable.")
        cls._status = cls._status_class()  # pylint: disable=not-callable
        if not callable(cls._config_class):
            raise TypeError("Configuration class not callable.")
        cls._config = cls._config_class(app_location)  # pylint: disable=not-callable
        LOG.debug("app initialized")

    @classmethod
    async def db_ready(cls):
        "Run when DB is ready"
        await cls._config.get_configuration_from_db()

    @_classproperty
    def status_object(self) -> GlobalStatus:
        """The app's status object.
        This should only be used for calling status methods.
        """
        return self._status

    @_classproperty
    def status(self) -> StatusEnum:
        "Global status of the app"
        return self._status.status

    @_classproperty
    def config_object(self) -> GlobalConfiguration:
        """The app's configuration object.
        This should only be used for calling config methods.
        """
        return self._config

    @_classproperty
    def configuration(self) -> dict:
        "Global configuration of the app"
        return self._config.configuration()

    @_classproperty
    def db(self) -> "core.db.db.DB":
        "Global DB object"
        return self._db

    @classmethod
    def set_db(cls, db: "core.db.db.DB"):
        "Set the global DB object"
        cls._db = db

    @classmethod
    def set_status_class(cls, status_cls: GlobalStatus, st_enum: StatusEnum):
        "set the class of the global Status object"
        cls._status_class = status_cls
        cls._status_enum_class = st_enum

    @classmethod
    def set_config_class(cls, cfg_cls: GlobalConfiguration, cfg_unum: ConfigEnum):
        "set the class of the global configuration object"
        cls._config_class = cfg_cls
        cls._config_enum_class = cfg_unum


logExit(LOG)
