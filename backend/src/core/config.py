""" Manage the configuration of the app
    - DB configuration is read from a file
    - other configuration is made persistent in the DB
    - user configurable attributes can be maintained by the frontend
"""

import json
from core.status import app, Status

LOCAL_CFG_FILE = "configuration.json"

config = {"user_cfg": {}, "db_cfg": {}}


def get_db_config():
    "read DB configuration from file"
    with open(LOCAL_CFG_FILE) as cfg_file:
        cfg = json.load(cfg_file)
        # config["db_cfg"]["url"] = cfg.get("url", None)
        config["db_cfg"] = {c: cfg.get(c) for c in ("host", "user", "password", "db")}
        if config["db_cfg"].get("db") is None:
            app.status = Status.STATUS_NO_DB
    return config["db_cfg"]
