"""Manage the configuration of the app
- initially configuration is read from a file
# - when DB is connected the initial configuration is merged
#     with the DB persisted configuration
# - configuration is made persistent in the DB
# - user configurable attributes can be maintained by the frontend
"""

import copy
import pprint
from typing import Optional, Any

from core.app_logging import getLogger, log_exit, redact, VERBOSE_DEBUG
from core.util_base import update_dicts_recursively

LOG = getLogger(__name__)

from core.configuration.cmd_line import CommandLine
from core.configuration.file_config import FileConfig
from core.const import SINGLE_USER_NAME
from core.util_base import get_config_item
from core.configuration.setup_config import SetupConfigValues
from core.app import App
from core.status import Status
from core.reconfigure_logging import reconfigure_logging
from core.exceptions import ConfigurationError
from core.base_objects import ConfigurationBaseClass, Config, ConfigDict
from business_objects.business_object_base import BOBase
from data.management.configuration import Configuration
from data.management.user import User, UserRole
from transient_data.cmdline_configuration import CmdlineConfiguration
from transient_data.file_configuration import FileConfiguration
from database.sql_expression import ColumnName


class AppConfiguration(ConfigurationBaseClass):
    "read app configuration"

    def __init__(self, app_location: str) -> None:
        super().__init__(app_location)
        self._cmdline_configuration: Optional[CmdlineConfiguration] = None
        self._file_configuration: Optional[FileConfiguration] = None
        self._global_configuration: Optional[Configuration] = None

    def cmdline_configuration(self) -> ConfigDict:
        "Config parsed from commandline"
        if self._cmdline_configuration:
            return self._cmdline_configuration.configuration or {}
        return {}

    async def config_change_handler(self, _: BOBase):
        """Handle events from the configuration business objects.
        This is needed to react to changes in the configuration,
        e.g. to reconfigure logging when the log level is changed in the configuration.
        """
        LOG.debug(
            "AppConfiguration._handle_event_: Configuration changed, reconfiguring logging."
        )
        reconfigure_logging()

    def initialize_configuration(self):
        """Initialize App configuration
        - parse commandline for configuration overrides
        - either:
            - set DB configuration from commandline
            - read DB configuration from config file
        """
        LOG.debug("AppConfiguration.initialize_configuration()")
        self._cmdline_configuration = CmdlineConfiguration()
        self._cmdline_configuration.subscribe_to_instance(self.config_change_handler)
        self._file_configuration = FileConfiguration(
            cmdline_config=self._cmdline_configuration.configuration
        )
        self._file_configuration.subscribe_to_instance(self.config_change_handler)
        reconfigure_logging()

    async def get_configuration_from_db(self):
        "Fetch configuration from database"
        async with FileConfig.db_config_lock:
            LOG.debug("AppConfiguration.get_configuration_from_db: DB available")
            config_ids = await Configuration.get_matching_ids(
                {ColumnName("user_id"): None}
            )
            LOG.log(
                VERBOSE_DEBUG,
                f"AppConfiguration.get_configuration_from_db: {config_ids=}",
            )
            if len(config_ids) == 0:
                LOG.info(
                    "Creating global configuration "
                    f"{str(Config.CONFIG_USR_MODE)}={SetupConfigValues.SINGLE_USER}"
                )
                configuration = Configuration(
                    configuration={
                        Config.CONFIG_APP: {
                            Config.CONFIG_USR_MODE: SetupConfigValues.SINGLE_USER
                        }
                    }
                )
                if not isinstance(configuration, Configuration):
                    raise ConfigurationError("Cannot create global configuration.")
                self._global_configuration = configuration
                await self._global_configuration.store()
            elif len(config_ids) == 1:
                self._global_configuration = await Configuration().fetch(
                    id=config_ids[0]
                )
            else:
                raise ConfigurationError("Multiple global configurations in DB.")
            self._global_configuration.subscribe_to_instance(self.config_change_handler)
            if LOG.isEnabledFor(VERBOSE_DEBUG):
                LOG.log(VERBOSE_DEBUG, f"AppConfiguration.get_configuration_from_db:")
                for line in pprint.pformat(
                    redact(self._global_configuration.configuration_dict),
                    indent=4,
                    width=120,
                    compact=True,
                ).splitlines():
                    LOG.log(VERBOSE_DEBUG, f" - {line}")

            user_mode = get_config_item(
                self._global_configuration.configuration_dict, Config.CONFIG_APP_USRMODE
            )
            if user_mode not in [
                SetupConfigValues.SINGLE_USER,
                SetupConfigValues.MULTI_USER,
            ]:
                self._global_configuration = None
                raise ConfigurationError(f"User mode: '{user_mode}'")
            if (
                user_mode == SetupConfigValues.SINGLE_USER
                and not await User.get_matching_ids(
                    {ColumnName("name"): SINGLE_USER_NAME}
                )
            ):
                # create single user
                LOG.info(f"Creating single user '{SINGLE_USER_NAME}'")
                await User(name=SINGLE_USER_NAME, role=UserRole.ADMIN).store()
            LOG.debug(f"AppConfiguration.get_configuration_from_db: {user_mode=}")
            App.status = (
                Status.STATUS_SINGLE_USER
                if user_mode == SetupConfigValues.SINGLE_USER
                else Status.STATUS_MULTI_USER
            )

    def configuration(self) -> dict[str, Any]:
        "global configuration"
        if self._global_configuration:
            cfg = copy.deepcopy(self._global_configuration.configuration_dict)
        else:
            cfg = {}
        if self._file_configuration:
            update_dicts_recursively(cfg, self._file_configuration.configuration or {})
        if self._cmdline_configuration:
            update_dicts_recursively(
                cfg, self._cmdline_configuration.configuration or {}
            )
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            LOG.debug("AppConfiguration.configuration():")
            for line in pprint.pformat(
                redact(cfg), indent=4, width=120, compact=True
            ).splitlines():
                LOG.log(VERBOSE_DEBUG, f" - {line}")
        return cfg


App.set_config_class(AppConfiguration, Config)

log_exit(LOG)
