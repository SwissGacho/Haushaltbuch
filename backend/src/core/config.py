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

from core.app import App
from core.setup_config import parse_commandline, cfg_searchpaths
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Config(StrEnum):
    "Configuration keys"
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


class ConfigIndex(StrEnum):
    CFGIX_SEARCHPATH = "search_path"


class AppConfiguration:
    "read app configuration"

    def __init__(self, app_location: str) -> None:
        self._cmdline_configuration = None
        self._setup_configuration = None
        self._global_configuration = None
        self._user_configuration = None
        self._app_location = app_location

    def _read_db_config_file(self, cfg_searchpath: list[Path]) -> dict:
        dbcfg_file = Path(self._cmdline_configuration.get(Config.CONFIG_DBCFG_FILE))
        LOG.debug(f"{dbcfg_file=}")
        try:
            for filename in (
                [dbcfg_file]
                if dbcfg_file.is_absolute()
                else [Path(path, dbcfg_file) for path in (cfg_searchpath)]
            ):
                LOG.debug(f"{filename=}")
                try:
                    with open(filename, encoding="utf-8") as cfg_file:
                        cfg = json.load(cfg_file)
                    LOG.debug(f"{cfg=}")
                    return cfg
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
        LOG.debug(
            f"AppConfiguration._init_setup_configuration() -> {self._setup_configuration}"
        )
        return self._setup_configuration

    def configuration(self) -> dict:
        "global configuration"
        return (
            self._global_configuration or self._init_setup_configuration()
        ) | self._cmdline_configuration

    def write_db_cfg_file(self, filename: str):
        "Write the DB configuration to the indicated file"
        with open(file=filename, mode="w") as cfg_file:
            LOG.debug(f"{filename=}")
            LOG.debug(
                f"{json.dumps({Config.CONFIG_DB: self._setup_configuration.get(Config.CONFIG_DB)})=}"
            )
            cfg_file.write(
                json.dumps(
                    {Config.CONFIG_DB: self._setup_configuration.get(Config.CONFIG_DB)}
                )
            )

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
