""" Manage the configuration of the app
    - initially configuration is read from a file
    # - when DB is connected the initial configuration is merged 
    #     with the DB persisted configuration
    # - configuration is made persistent in the DB
    # - user configurable attributes can be maintained by the frontend
"""

from enum import Enum
import json
import logging
from core.app_logging import getLogger

LOG = getLogger(__name__)

LOCAL_CFG_FILE = "configuration.json"


class Config(Enum):
    CONFIG_DB = "db_cfg"
    CONFIG_DB_DB = "db"


class AppConfiguration:
    "read app configuration"

    def __init__(self) -> None:
        try:
            with open(LOCAL_CFG_FILE) as cfg_file:
                cfg = json.load(cfg_file)
        except:
            LOG.warning(f"Unable to read configuration from {LOCAL_CFG_FILE}")
            cfg = {}
        self.configuration = cfg
        LOG.info(f"configuration: {self.configuration}")
