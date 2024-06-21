""" Manage the configuration of the app
    - initially configuration is read from a file
    # - when DB is connected the initial configuration is merged 
    #     with the DB persisted configuration
    # - configuration is made persistent in the DB
    # - user configurable attributes can be maintained by the frontend
"""

from enum import StrEnum
from pathlib import Path
import json
import logging
from core.app_logging import getLogger

LOG = getLogger(__name__)

LOCAL_CFG_FILE = "configuration.json"


class Config(StrEnum):
    CONFIG_DB = "db_cfg"
    CONFIG_DB_FILE = "file"
    CONFIG_DB_HOST = "host"
    CONFIG_DB_DB = "db"
    CONFIG_DB_USER = "user"
    CONFIG_DB_PW = "password"
    CONFIG_CFG_SEARCH_PATH = "config_search_path"


class ConfigIndex(StrEnum):
    CFGIX_SEARCHPATH = "search_path"


class AppConfiguration:
    "read app configuration"

    def __init__(self, app_location: str) -> None:
        self.configuration = {
            Config.CONFIG_CFG_SEARCH_PATH: [
                Path.cwd(),
                Path(app_location).parent,
                Path.home(),
            ]
        }
        try:
            with open(LOCAL_CFG_FILE) as cfg_file:
                cfg = json.load(cfg_file)
        except FileNotFoundError:
            LOG.info(f"configuration file {LOCAL_CFG_FILE} not found.")
            cfg = {}
        except Exception as exc:
            LOG.warning(f"Unable to read configuration from {LOCAL_CFG_FILE}: {exc}")
            cfg = {}
        self.configuration |= cfg
        LOG.debug(f"configuration: {self.configuration}")

    def handle_fetch_configuration(self, index: str) -> str:
        "return a requested part of the configuration"
        if index == ConfigIndex.CFGIX_SEARCHPATH:
            return [
                str(p) for p in self.configuration.get(Config.CONFIG_CFG_SEARCH_PATH)
            ]
        return ""


# LOG.debug("module imported")
