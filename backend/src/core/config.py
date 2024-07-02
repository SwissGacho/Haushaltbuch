""" Manage the configuration of the app
    - initially configuration is read from a file
    # - when DB is connected the initial configuration is merged 
    #     with the DB persisted configuration
    # - configuration is made persistent in the DB
    # - user configurable attributes can be maintained by the frontend
"""

from pathlib import Path
from enum import StrEnum
import platform
import json
import asyncio

from data.management.configuration import Configuration
from core.app import App
from core.setup_config import parse_commandline, cfg_searchpaths
from core.status import Status
from core.app_logging import getLogger

LOG = getLogger(__name__)
WAIT_AVAILABLE_TASK = "wait_for_available"
WAIT_FAILURE_TASK = "wait_for_failure"


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


class _SetupConfigKeys(StrEnum):
    CONFIG = "configuration"
    CFG_APP = "configuration/app"
    DBCFG_CFG_FILE = "dbcfg_file"
    # DBCFG = "db_cfg"
    CFG_DBCFG = "configuration/db_cfg"
    APP = "app"
    ADM_USER = "adminuser"


class ConfigIndex(StrEnum):
    CFGIX_SEARCHPATH = "search_path"


class AppConfiguration:
    "read app configuration"

    def __init__(self, app_location: str) -> None:
        self._cmdline_configuration = None
        self._setup_configuration = None
        self._global_configuration: Configuration = None
        self._user_configuration = None
        self._app_location = app_location

    def _get_config_item(self, cfg: dict, key: Config):
        for key_part in key.split("/"):
            cfg = cfg.get(key_part)
        return cfg

    async def _init_configuration(self):
        LOG.debug("AppConfiguration._init_configuration: DB available")
        self._global_configuration = await Configuration().fetch(newest=True)
        user_mode = self._get_config_item(
            self._global_configuration.configuration, Config.CONFIG_APP_USRMODE
        )
        LOG.debug(f"AppConfiguration._init_configuration: {user_mode=}")
        App.status = (
            Status.STATUS_SINGLE_USER
            if user_mode == "single"
            else Status.STATUS_MULTI_USER
        )

    def _read_db_config_file(
        self, cfg_searchpath: list[Path], dbcfg_filename: str = None
    ) -> dict:
        dbcfg_file = Path(
            dbcfg_filename or self._cmdline_configuration.get(Config.CONFIG_DBCFG_FILE)
        )
        # LOG.debug(f"AppConfiguration._read_db_config_file: {dbcfg_file=}")
        try:
            for filename in (
                [dbcfg_file]
                if dbcfg_file.is_absolute()
                else [Path(path, dbcfg_file) for path in (cfg_searchpath)]
            ):
                # LOG.debug(f"Searching file: {str(filename)}")
                try:
                    with open(filename, encoding="utf-8") as cfg_file:
                        db_config_from_cfg_file = json.load(cfg_file)
                    # LOG.debug(f"Found DB configuration: {db_config_from_cfg_file}")
                    return db_config_from_cfg_file
                except FileNotFoundError:
                    continue
            LOG.info(f"configuration file {dbcfg_file} not found.")
            return {}
        except json.JSONDecodeError as exc:
            LOG.warning(f"Unable to decode configuration from {dbcfg_file}: {exc}")
            return {}
        except (IsADirectoryError, NotADirectoryError, PermissionError, OSError) as exc:
            LOG.warning(f"Unable to read configuration from {dbcfg_file}: {exc}")
            return {}

    def _init_setup_configuration(self) -> dict:
        # LOG.debug("AppConfiguration._init_setup_configuration()")
        if self._setup_configuration is not None:
            return self._setup_configuration

        cfg_searchpath, db_locations = cfg_searchpaths(self._app_location)

        self._cmdline_configuration = parse_commandline(Config.CONFIG_DBCFG_FILE)
        # LOG.debug(f"AppConfiguration: {self._cmdline_configuration=}")
        if self._cmdline_configuration.get(Config.CONFIG_DB, {}).get(
            Config.CONFIG_DB_DB
        ):
            LOG.info(
                "AppConfiguration._init_setup_configuration: found DB configuration on commandline"
            )
            self._setup_configuration = {}
            return self._setup_configuration
        self._setup_configuration = {
            Config.CONFIG_CFG_SEARCH_PATH: cfg_searchpath,
            Config.CONFIG_DB_LOCATIONS: db_locations,
            Config.CONFIG_SYSTEM: platform.system(),
        }
        self._setup_configuration |= self._read_db_config_file(cfg_searchpath)
        # LOG.debug(f"AppConfiguration._init_setup_configuration() -> {self._setup_configuration}")
        return self._setup_configuration

    def configuration(self) -> dict:
        "global configuration"
        cfg = (
            self._global_configuration or self._init_setup_configuration()
        ) | self._cmdline_configuration
        LOG.debug(f"AppConfiguration.configuration() -> {cfg}")
        return cfg

    async def _wait_for_db(self) -> bool:
        LOG.debug("Request DB restart.")
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

    async def setup_configuration(self, setup_cfg: dict):
        """Setup configuration.
        -  write DB configuration file
        -  create configuration business object
        """
        db_filename = self._get_config_item(setup_cfg, _SetupConfigKeys.DBCFG_CFG_FILE)
        db_cfg = {
            Config.CONFIG_DB: self._get_config_item(
                setup_cfg, _SetupConfigKeys.CFG_DBCFG
            )
        }
        # LOG.debug(f"AppConfiguration.setup_configuration({setup_cfg=}")
        # pylint: disable=unspecified-encoding
        with open(file=db_filename, mode="w") as cfg_file:
            cfg_file.write(json.dumps(db_cfg))
        self._setup_configuration |= self._read_db_config_file(
            [Path(db_filename).parent], Path(db_filename).name
        )
        # self._setup_configuration |= db_cfg
        configuration = {
            Config.CONFIG_APP: self._get_config_item(
                setup_cfg, _SetupConfigKeys.CFG_APP
            )
        }
        if await self._wait_for_db():
            bo = Configuration(cfg=configuration)
            LOG.debug(f"{bo=}")
            await bo.store()
            LOG.debug(f"{bo=}")
        else:
            LOG.error("Start DB failed with new configuration.")

        if self._get_config_item(configuration, Config.CONFIG_APP_USRMODE) == "multi":
            adm_user = self._get_config_item(setup_cfg, _SetupConfigKeys.ADM_USER)

    def handle_fetch_configuration(self, index: str) -> str:
        "return a requested part of the configuration"
        if index == ConfigIndex.CFGIX_SEARCHPATH:
            return [
                str(p)
                for p in self._setup_configuration.get(Config.CONFIG_CFG_SEARCH_PATH)
            ]
        return ""


App.set_config_class(AppConfiguration, Config)
# LOG.debug("module imported")
