""" Applications base object"""

from enum import StrEnum
from typing import Optional


class BaseObject:
    """No functionality required (yet).
    This class allows classes to be identified to belong to the app.
    """

    def __init__(self, *args, **kwargs) -> None:
        pass


class Status(StrEnum):
    STATUS_UNCONFIGURED = "unconfigured"
    STATUS_ONLINE = "online"
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
    CONFIG_APP_USRMODE = "app/userMode"
    CONFIG_DBCFG_FILE = "dbcfg_file"
    CONFIG_DB = "db_cfg"
    CONFIG_DB_FILE = "file"
    CONFIG_DB_HOST = "host"
    CONFIG_DB_DB = "db"
    CONFIG_DB_USER = "user"
    CONFIG_DB_PW = "password"
    CONFIG_CFG_SEARCH_PATH = "config_search_path"
    CONFIG_SYSTEM = "system"
    CONFIG_DB_LOCATIONS = "db_paths"
    CONFIG_ADMINUSER = "adminuser"


class ConfigurationBaseClass(BaseObject):
    "Configuration Baseclass"

    def __init__(self, app_location: str) -> None:
        pass

    async def get_configuration_from_db(self):
        "implemented in AppConfiguration"
        pass

    def configuration(self) -> dict:
        "global configuration"
        return {}


class DBBaseClass(BaseObject):
    "DB Baseclass"
