""" Manage connection to the database
"""

from contextlib import asynccontextmanager

from core.app import App
from core.status import Status
from core.config import Config
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DBRestart(Exception):
    pass


@asynccontextmanager
async def get_db():
    "Create a DB connection"
    if App.status == Status.STATUS_DB_CFG:
        db_config = App.configuration[Config.CONFIG_DB]
        # LOG.debug(f"DB configuration: {db_config.keys()=}")
        if db_config.keys() == {Config.CONFIG_DB_FILE}:
            LOG.debug("Connect to SQLite")
            from db.sqlite import SQLiteDB

            db = SQLiteDB(**App.configuration[Config.CONFIG_DB])
        elif db_config.keys() == {
            Config.CONFIG_DB_HOST,
            Config.CONFIG_DB_DB,
            Config.CONFIG_DB_USER,
            Config.CONFIG_DB_PW,
        }:
            LOG.info("Connect to MySQL DB")
            from db.mysql import MySQLDB

            db = MySQLDB(**App.configuration[Config.CONFIG_DB])
        else:
            LOG.warning(f"Invalid DB configuration: {db_config}")
            db = None
        if db:
            try:
                await db.check()
                LOG.debug("DB ready")
                yield db
            finally:
                LOG.debug("DB disconnecting")
                await db.close()

    else:
        LOG.warning("No DB configuration available")
    # yield None


# LOG.debug("module imported")
