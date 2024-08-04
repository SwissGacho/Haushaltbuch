""" Manage the configuration of the app
    - initially configuration is read from a file
    # - when DB is connected the initial configuration is merged 
    #     with the DB persisted configuration
    # - configuration is made persistent in the DB
    # - user configurable attributes can be maintained by the frontend
"""

from typing import Optional

from core.configuration.cmd_line import parse_commandline
from core.configuration.db_config import DBConfig
from core.util import get_config_item
from core.configuration.setup_config import SetupConfigValues
from core.app import App
from core.status import Status
from core.app_logging import getLogger
from core.exceptions import ConfigurationError
from core.base_objects import ConfigurationBaseClass, Config, ConfigDict
from data.management.configuration import Configuration
from database.sqlexpression import ColumnName

LOG = getLogger(__name__)


class AppConfiguration(ConfigurationBaseClass):
    "read app configuration"

    def __init__(self, app_location: str) -> None:
        super().__init__(app_location)
        self._cmdline_configuration: Optional[ConfigDict] = None
        self._global_configuration: Optional[Configuration] = None
        self._user_configuration: Optional[Configuration] = None

    def cmdline_configuration(self) -> ConfigDict:
        "Config parsed from commandline"
        return self._cmdline_configuration or {}

    def initialize_configuration(self):
        "Parse commandline for configuration overrides"
        LOG.debug("AppConfiguration.initialize_configuration()")
        self._cmdline_configuration = parse_commandline(Config.CONFIG_DBCFG_FILE)
        if Config.CONFIG_DB in self._cmdline_configuration:
            DBConfig.set_db_configuration(
                {Config.CONFIG_DB: self._cmdline_configuration[Config.CONFIG_DB]}
            )
        else:
            DBConfig.read_db_config_file()

    async def get_configuration_from_db(self):
        "Fetch configuration from database"
        async with DBConfig.db_config_lock:
            # LOG.debug("AppConfiguration.get_configuration_from_db: DB available")
            config_ids = await Configuration.get_matching_ids(
                {ColumnName("user_id"): None}
            )
            if len(config_ids) != 1:
                raise ConfigurationError("Multiple or no global configurations")
            self._global_configuration = await Configuration().fetch(id=config_ids[0])
            user_mode = get_config_item(
                self._global_configuration.configuration_dict, Config.CONFIG_APP_USRMODE
            )
            if not user_mode in [
                SetupConfigValues.SINGLE_USER,
                SetupConfigValues.MULTI_USER,
            ]:
                self._global_configuration = None
                raise ConfigurationError(f"User mode: '{user_mode}'")
            # LOG.debug(f"AppConfiguration.get_configuration_from_db: {user_mode=}")
            App.status_object.status = (
                Status.STATUS_SINGLE_USER
                if user_mode == SetupConfigValues.SINGLE_USER
                else Status.STATUS_MULTI_USER
            )

    def configuration(self) -> dict:
        "global configuration"
        if self._global_configuration:
            global_cfg_dict = self._global_configuration.configuration_dict
        else:
            global_cfg_dict = {}
        cfg = global_cfg_dict | (self._cmdline_configuration or {})
        # LOG.debug(f"AppConfiguration.configuration() -> {cfg}")
        return cfg


App.set_config_class(AppConfiguration, Config)
# LOG.debug("module imported")
