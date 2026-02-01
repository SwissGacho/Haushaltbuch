"""Common constants"""

from asyncio import Event
from typing import Type, Self, Optional

# pylint: disable=wrong-import-position
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.util import _classproperty
from core.base_objects import (
    StatusBaseClass,
    Status,
    ConfigurationBaseClass,
    Config,
    ConfigDict,
    DBBaseClass,
)


class App:
    "keep status and configuration of the app"

    _status_class: type[StatusBaseClass] | None = None
    _status_enum_class: type[Status] | None = None
    _config_class: type[ConfigurationBaseClass] | None = None
    _config_enum_class: type[Config] | None = None
    db_available = Event()
    db_failure = Event()
    db_restart = Event()
    db_request_restart = Event()

    _status: Optional[StatusBaseClass] = None
    _config: Optional[ConfigurationBaseClass] = None
    _db: Optional[DBBaseClass] = None

    @classmethod
    def initialize(cls, app_location: str, app_version: Optional[str] = None):
        "Initialize global objects (Status, Config)"
        if cls._status_class is None:
            raise TypeError("Status class not initialized.")
        else:
            cls._status = cls._status_class(version=app_version)
        if cls._config_class is None:
            raise TypeError("Configuration class not initialized.")
        else:
            cls._config = cls._config_class(app_location)
        cls._config.initialize_configuration()
        # LOG.debug("app initialized")

    @classmethod
    async def db_ready(cls):
        "Run when DB is ready"
        if not cls._config:
            raise ReferenceError("Status and Configuration not initialized")
        await cls._config.get_configuration_from_db()

    # pylint: disable=no-self-argument
    @_classproperty
    def status_object(
        cls: Type[Self],  # type: ignore[reportGeneralTypeIssues]
    ) -> StatusBaseClass:
        """The app's status object.
        This should only be used for calling status methods.
        """
        if cls._status is None:
            raise ReferenceError("Status and Configuration not initialized")
        return cls._status

    # pylint: disable=no-self-argument
    @_classproperty
    def status(  # type: ignore [reportRedeclaration]
        cls: Type[Self],  # type: ignore[reportGeneralTypeIssues]
    ) -> Status:
        "Global status of the app"
        if cls._status is None:
            raise ReferenceError("Status and Configuration not initialized")
        return cls._status.status

    @status.setter
    def status(cls, value: Status):
        if cls._status is None:
            raise ReferenceError("Status and Configuration not initialized")
        cls._status.status = value  # type: ignore

    @classmethod
    def config_object(cls) -> ConfigurationBaseClass:
        """The app's configuration object.
        This should only be used for calling config methods.
        """
        if not cls._config:
            raise ReferenceError("Status and Configuration not initialized")
        return cls._config

    # pylint: disable=no-self-argument
    @_classproperty
    def configuration(
        cls: Type[Self],  # type: ignore[reportGeneralTypeIssues]
    ) -> ConfigDict:
        "Global configuration of the app"
        if not cls._config:
            raise ReferenceError("Status and Configuration not initialized")
        return cls._config.configuration()

    @classmethod
    def get_config_item(cls, key: Config, default=None):
        "Extract an item from global configuration"
        if not cls._config:
            raise ReferenceError("Status and Configuration not initialized")
        return cls._config.configuration().get(key, default)

    # pylint: disable=no-self-argument
    @_classproperty
    def db(
        cls: Type[Self],  # type: ignore[reportGeneralTypeIssues]
    ) -> DBBaseClass:
        "Global DB object"
        if not cls._db:
            raise ReferenceError("DB not initialized")
        return cls._db

    @classmethod
    def set_db(cls, db: DBBaseClass):
        "Set the global DB object"
        cls._db = db

    @classmethod
    def set_status_class(cls, status_cls: type[StatusBaseClass], st_enum: type[Status]):
        "set the class of the global Status object"
        cls._status_class = status_cls
        cls._status_enum_class = st_enum

    @classmethod
    def set_config_class(
        cls, cfg_cls: type[ConfigurationBaseClass], cfg_unum: type[Config]
    ):
        "set the class of the global configuration object"
        cls._config_class = cfg_cls
        cls._config_enum_class = cfg_unum


log_exit(LOG)
