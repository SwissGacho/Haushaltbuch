""" Manage connection to the database
"""

from contextlib import asynccontextmanager

from core.app import App
from core.util import get_config_item
from core.status import Status
from core.configuration.config import Config
from core.configuration.db_config import DBConfig
from core.app_logging import getLogger
from database.sqlite import SQLiteDB
from database.mysql import MySQLDB
from database.schema_maintenance import check_db_schema


LOG = getLogger(__name__)


@asynccontextmanager
async def get_db():
    "Create a DB connection"
    if App.status != Status.STATUS_DB_CFG:  # pylint: disable=comparison-with-callable
        LOG.warning("No DB configuration available")
        yield
        return

    db_config = get_config_item(DBConfig.db_configuration, Config.CONFIG_DB)
    db_type = get_config_item(DBConfig.db_configuration, Config.CONFIG_DB_DB)
    if not (db_config and db_type):
        LOG.error(f"Invalid DB configuration: {App.configuration}")
        yield
        return
    # LOG.debug(f"DB configuration: {db_config=}, {db_type=}")
    if db_type == "SQLite":
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
    elif db_type == "MySQL":
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
