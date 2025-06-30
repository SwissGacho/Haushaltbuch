"""Applications base classes and common objects."""

from typing import TypeAlias, Union
from enum import StrEnum
import platform


class BaseObject:
    """No functionality required (yet).
    This class allows classes to be identified to belong to the app.
    """

    def __init__(self, *args, **kwargs) -> None:
        pass


class Status(StrEnum):
    "Values for global app status"

    STATUS_UNCONFIGURED = "unconfigured"
    STATUS_NO_DB = "noDB"
    STATUS_DB_CFG = "DBconfigured"
    STATUS_DB_UNSUPPORTED = "DBunsuppoerted"
    STATUS_CHECK_DB = "checkingDBschema"
    STATUS_OLD_DB = "outdatedDBschema"
    STATUS_SINGLE_USER = "singleUser"
    STATUS_MULTI_USER = "multiUser"


class StatusBaseClass(BaseObject):
    "Status Baseclass"

    @property
    def status(self) -> Status:
        "Current status of the app"
        return Status.STATUS_UNCONFIGURED


class Config(StrEnum):
    "Configuration keys"

    CONFIG_APP = "app"
    CONFIG_USR_MODE = "userMode"
    CONFIG_APP_USRMODE = "/".join([CONFIG_APP, CONFIG_USR_MODE])
    CONFIG_DBCFG_FILE = "dbcfg_file"
    CONFIG_DB = "db_cfg"
    CONFIG_DB_DB = "/".join([CONFIG_DB, "db"])
    CONFIG_DBFILE = "file"
    CONFIG_DBHOST = "host"
    CONFIG_DBDBNAME = "dbname"
    CONFIG_DBUSER = "dbuser"
    CONFIG_DBPW = "password"
    CONFIG_CFG_SEARCH_PATH = "config_search_path"
    CONFIG_SYSTEM = "system"
    CONFIG_DB_LOCATIONS = "db_paths"
    CONFIG_ADMINUSER = "adminuser"


ConfigSubDict: TypeAlias = Union[dict[str, "ConfigSubDict"], str]
ConfigDict: TypeAlias = dict[str, "ConfigSubDict"]


class ConfigurationBaseClass(BaseObject):
    "Configuration Baseclass"

    def __init__(self, app_location: str) -> None:
        self.app_location = app_location
        self.system = platform.system()

    def initialize_configuration(self):
        "Parse commandline for configuration overrides"

    async def get_configuration_from_db(self):
        "implemented in AppConfiguration"

    def configuration(self) -> dict:
        "global configuration"
        return {}


class DBBaseClass(BaseObject):
    "DB Baseclass"

    @property
    def sql_factory(self):
        "DB specific SQL factory"
        raise NotImplementedError("sqlFactory not defined on base class")

    async def connect(self) -> "ConnectionBaseClass":
        "Open a connection and return the Connection instance"
        raise ConnectionError("Called from DB base class.")

    async def execute(
        self,
        query: str,
        params=None,
        connection: "ConnectionBaseClass" = None,
    ):
        """Open a connection, execute a query and return the Cursor instance.
        If 'close'=True close connection after fetching all rows"""
        raise NotImplementedError("execute not implemented in base class.")

    async def close(self):
        "close all activities"


class ConnectionBaseClass(BaseObject):
    "Connection Baseclass"

    async def connect(self):
        "Open a connection and return the Connection instance"
        raise ConnectionError("Called from DB base class.")

    async def close(self):
        "close the connection"
        raise ConnectionError("Called from DB base class.")
