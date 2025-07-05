"""Manage connection to the database"""

import importlib
import pkgutil
from contextlib import asynccontextmanager

from core.app import App
from core.util import get_config_item
from core.status import Status
from core.configuration.config import Config
from core.configuration.db_config import DBConfig
from core.app_logging import getLogger
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
    # LOG.debug(f"DB configuration: {db_config=}, {db_type=}")
    if not (db_config and db_type):
        LOG.error(f"Invalid DB configuration: { App.configuration}")
        yield
        return

    db = None
    dbms_package = "database.dbms"
    for _, name, _ in pkgutil.iter_modules(
        importlib.import_module(dbms_package).__path__
    ):
        module_name = f"{dbms_package}.{name}"
        LOG.debug(f"Trying to load DB module '{module_name}'")
        try:
            module = importlib.import_module(module_name)
            get_db_func = getattr(module, "get_db", None)
            if callable(get_db_func):
                db = get_db_func(db_type, **db_config)
                LOG.info(f"Connect to {db_type}")
                if db:
                    break
        except Exception as exc:
            LOG.error(f"Error loading DB module '{module_name}': {exc}")
    if not db:
        App.status = Status.STATUS_DB_UNSUPPORTED
        LOG.warning(f"Invalid DB configuration or no suitable DBMS found: {db_config}")
        yield
        return

    try:
        App.db = db
        await check_db_schema()
        LOG.debug("DB ready")
        yield db
    except TypeError as exc:
        LOG.error(f"DB connection failed: {exc}")
        App.status = Status.STATUS_NO_DB
        yield
        return
    finally:
        LOG.debug("DB disconnecting")
        await db.close()
