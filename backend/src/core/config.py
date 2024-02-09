""" Manage the configuration of the app
    - initially configuration is read from a file
    # - when DB is connected the initial configuration is merged 
    #     with the DB persisted configuration
    # - configuration is made persistent in the DB
    # - user configurable attributes can be maintained by the frontend
"""

from enum import StrEnum
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


class AppConfiguration:
    "read app configuration"

    def __init__(self) -> None:
        try:
            with open(LOCAL_CFG_FILE) as cfg_file:
                cfg = json.load(cfg_file)
        except FileNotFoundError:
            LOG.info(f"configuration file {LOCAL_CFG_FILE} not found.")
            cfg = {}
        except Exception as exc:
            LOG.warning(f"Unable to read configuration from {LOCAL_CFG_FILE}: {exc}")
            cfg = {}
        self.configuration = cfg
        LOG.debug(f"configuration: {self.configuration}")


# LOG.debug("module imported")
