""" Manage connection to the database
"""

from contextlib import asynccontextmanager

from core.app import App
from core.status import Status
from core.config import Config
from db.sqlite import SQLiteDB
from db.mysql import MySQLDB
from db.schema_maintenance import check_db_schema

# from persistance.business_object_base import BO_Base
from data.management.db_schema import DB_Schema
from core.app_logging import getLogger

LOG = getLogger(__name__)


@asynccontextmanager
async def get_db():
    "Create a DB connection"
    if App.status != Status.STATUS_DB_CFG:
        LOG.warning("No DB configuration available")
        yield
        return

    db_config = App.configuration[Config.CONFIG_DB]
    # LOG.debug(f"DB configuration: {db_config.keys()=}")
    if db_config.keys() == {Config.CONFIG_DB_FILE}:
        LOG.debug("Connect to SQLite")
        try:
            db = SQLiteDB(**db_config)
        except ModuleNotFoundError as exc:
            App.status = Status.STATUS_DB_UNSUPPORTED
            LOG.error(f"{exc}")
            if "aiosqlite" in str(exc):
                LOG.error(
                    "Library 'aiosqlite' could not be imported. "
                    "Please install using 'pip install aiosqlite'"
                )
            yield
            return
    elif db_config.keys() == {
        Config.CONFIG_DB_HOST,
        Config.CONFIG_DB_DB,
        Config.CONFIG_DB_USER,
        Config.CONFIG_DB_PW,
    }:
        LOG.info("Connect to MySQL DB")
        try:
            db = MySQLDB(**db_config)
        except ModuleNotFoundError as exc:
            App.status = Status.STATUS_DB_UNSUPPORTED
            LOG.error(f"{exc}")
            if "aiomysql" in str(exc):
                LOG.error(
                    "Library 'aiomysql' could not be imported. "
                    "Please install using 'pip install aiomysql'"
                )
            yield
            return
    else:
        App.status = Status.STATUS_DB_UNSUPPORTED
        LOG.warning(f"Invalid DB configuration: {db_config}")
        yield
        return
    try:
        App.db = db
        await check_db_schema()
        LOG.debug("DB ready")
        yield db
    except TypeError:
        App.status = Status.STATUS_NO_DB
        yield
        return
    finally:
        LOG.debug("DB disconnecting")
        await db.close()
