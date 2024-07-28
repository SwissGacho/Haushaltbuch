""" Applications base classes and common objects. """

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
    CONFIG_APP_USRMODE = "/".join([CONFIG_APP, "userMode"])
    CONFIG_DBCFG_FILE = "dbcfg_file"
    CONFIG_DB = "db_cfg"
    CONFIG_DB_DB = "/".join([CONFIG_DB, "db"])
    CONFIG_DBFILE = "file"
    CONFIG_DBHOST = "host"
    CONFIG_DBUSER = "user"
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
