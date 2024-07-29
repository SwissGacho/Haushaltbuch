""" Helper functions for the configuration of the setup procedure """

from enum import StrEnum
from pathlib import Path
import json
import asyncio

from data.management.user import User, UserRole
from data.management.configuration import Configuration
from database.sqlexpression import ColumnName
from core.app import App
from core.configuration.util import get_config_item
from core.configuration.db_config import DBConfig
from core.base_objects import Config
from core.base_objects import BaseObject
from core.exceptions import ConfigurationError, DataError
from core.app_logging import getLogger

LOG = getLogger(__name__)

WAIT_AVAILABLE_TASK = "wait_for_available"
WAIT_FAILURE_TASK = "wait_for_failure"


class SetupConfigKeys(StrEnum):
    "Keys used by configuration setup"
    CONFIG = "configuration"
    CFG_APP = "configuration/app"
    DBCFG_CFG_FILE = "dbcfg_file"
    # DBCFG = "db_cfg"
    CFG_DBCFG = "configuration/db_cfg"
    APP = "app"
    ADM_USER = "adminuser"


class SetupConfigValues(StrEnum):
    "Values used by configuration setup"
    SINGLE_USER = "single"
    MULTI_USER = "multi"


class ConfigSetup(BaseObject):
    "Apply configuration from setup procedure"

    @classmethod
    async def _wait_for_db(cls) -> bool:
        # LOG.debug("Request DB restart.")
        App.db_request_restart.set()
        # LOG.debug("Wait for restart.")
        await App.db_restart.wait()
        # LOG.debug("Restart detected; wait for DB.")
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(App.db_available.wait(), name=WAIT_AVAILABLE_TASK),
                asyncio.create_task(App.db_failure.wait(), name=WAIT_FAILURE_TASK),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # LOG.debug("Restart done.")
        for task in pending:
            task.cancel()
        return WAIT_AVAILABLE_TASK in [t.get_name() for t in done]

    @classmethod
    async def setup_configuration(cls, setup_cfg: dict):
        """Setup configuration.
        -  write DB configuration file
        -  create configuration business object
        """
        # LOG.debug(f"ConfigSetup.setup_configuration({setup_cfg=}")
        db_filename = get_config_item(setup_cfg, SetupConfigKeys.DBCFG_CFG_FILE)
        if not isinstance(db_filename, str):
            raise TypeError(
                "DB configuration filename must be 'str' not '", type(db_filename), "'."
            )
        db_cfg = {
            Config.CONFIG_DB: get_config_item(setup_cfg, SetupConfigKeys.CFG_DBCFG)
        }
        # pylint: disable=unspecified-encoding
        with open(file=db_filename, mode="w") as cfg_file:
            cfg_file.write(json.dumps(db_cfg))
        DBConfig.read_db_config_file([Path(db_filename).parent], Path(db_filename).name)
        configuration = {
            Config.CONFIG_APP: get_config_item(setup_cfg, SetupConfigKeys.CFG_APP)
        }
        async with DBConfig.db_config_lock:
            if await cls._wait_for_db():
                rows_in_db = await Configuration.get_matching_ids(
                    {ColumnName("user_id"): None}
                )
                if len(rows_in_db) > 1:
                    LOG.error(
                        f"Multiple ({len(rows_in_db)}) global configurations in DB"
                    )
                    raise ConfigurationError("Multiple global configurations in DB")
                elif len(rows_in_db) == 1:
                    bo = await Configuration(id=rows_in_db[0]).fetch()
                    for key, set_cfg in configuration.items():
                        db_cfg = bo.configuration.get(key)
                        if set_cfg != db_cfg:
                            LOG.info(
                                f"Change configuration '{key}': {db_cfg} -> {set_cfg}"
                            )
                    bo.configuration = configuration
                else:
                    bo = Configuration(cfg=configuration)
                await bo.store()
            else:
                LOG.error("Start DB failed with new configuration.")

            if (
                get_config_item(configuration, Config.CONFIG_APP_USRMODE)
                == SetupConfigValues.MULTI_USER
            ):
                adm_user = get_config_item(setup_cfg, SetupConfigKeys.ADM_USER)
                if not isinstance(adm_user, dict):
                    raise TypeError(f"admin user must be dict not {type(adm_user)}")
                rows_in_db = await User.get_matching_ids(
                    {ColumnName("name"): adm_user["name"]}
                )
                if len(rows_in_db) > 1:
                    LOG.error(f"{len(rows_in_db)} users named {adm_user['name']} in DB")
                    raise DataError("Multiple users in DB")
                elif len(rows_in_db) == 1:
                    user = await User(id=rows_in_db[0]).fetch()
                    user.password = adm_user["password"]
                    user.role = UserRole.ROLE_ADMIN
                else:
                    user = User(
                        name=adm_user["name"],
                        password=adm_user["password"],
                        role=UserRole.ROLE_ADMIN,
                    )
                # LOG.debug(f"ConfigSetup.setup_configuration(): {user=}")
                await user.store()
