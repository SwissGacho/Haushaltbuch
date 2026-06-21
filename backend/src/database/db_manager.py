"""Manage connection to the database"""

from contextlib import asynccontextmanager
import importlib
from typing import AsyncContextManager, AsyncIterator, cast
import decimal

from core.app_logging import getLogger, log_exit, redact
from core.exceptions import ConfigurationError
from .dbms.db_base import DB

LOG = getLogger(__name__)

from core.app import App
from core.util_base import get_config_item
from core.status import Status
from core.configuration.config import Config
from database.schema_maintenance import check_db_schema

DB_TYPE_MAP: dict[str, tuple[str, str]] = {
    "SQLite": ("sqlite", "SQLiteDB"),
    "MySQL": ("mysql", "MySQLDB"),
    "MariaDB": ("mysql", "MySQLDB"),
}


class DBManager:
    "Initialize and configure the app database"

    def __init__(self, app: type[App]):
        self._app = app
        self.db: DB | None = None

    @property
    def _db_identifiers(self) -> tuple[dict | None, str | None]:
        return (
            get_config_item(self._app.configuration, Config.CONFIG_DB),
            cast(
                str | None,
                get_config_item(self._app.configuration, Config.CONFIG_DB_DB),
            ),
        )

    def _valid_db_config(self) -> tuple[dict, str, type[DB]]:
        db_config, db_type = self._db_identifiers
        if not (db_config and db_type):
            LOG.error(f"Invalid DB configuration: {redact(self._app.configuration)}")
            raise ConfigurationError("Invalid DB configuration")
        if not isinstance(db_type, str):
            LOG.error(f"Invalid DB type: {redact(db_type)}")
            raise ConfigurationError("Invalid DB type")
        if db_type not in DB_TYPE_MAP:
            LOG.error(f"Unsupported DB type: {db_type}")
            raise ConfigurationError(f"Unsupported DB type: {db_type}")
        db_class = self.import_db_by_name(*DB_TYPE_MAP[db_type])
        return db_config, db_type, db_class

    @asynccontextmanager
    async def get_db(self) -> AsyncIterator[DB | None]:
        "Get the database connection"
        app = self._app
        db_type: str | None = None
        if (
            app.status != Status.STATUS_DB_CFG
        ):  # pylint: disable=comparison-with-callable
            LOG.warning("No DB configuration available")
            yield None
            return

        try:
            db_config, db_type, db_class = self._valid_db_config()
        except ConfigurationError as exc:
            LOG.error(f"{exc}")
            self._set_db_unsupported()
            yield None
            return
        except ModuleNotFoundError as exc:
            missing_module = exc.name or "<unknown>"
            LOG.error(
                f"Library '{missing_module}' could not be imported for {db_type=}"
            )
            LOG.error(f"Please install using 'pip install {missing_module}'")
            LOG.error(f"{exc}")
            self._set_db_unsupported()
            yield None
            return

        LOG.debug(f"DB configuration: {redact(db_config)=}, {db_type=}")

        db = db_class(**db_config)

        # Post-connection configuration and schema check
        try:
            await self.connect_and_setup_db(db)
            yield db
        except (TypeError, ValueError) as exc:
            LOG.error(f"DB connection failed: {exc}")
            App.status = Status.STATUS_NO_DB
            yield None
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            LOG.warning(f"DB connection failed: {exc}")
        finally:
            LOG.debug("DB disconnecting")
            await db.close()

    def _set_db_unsupported(self):
        App.status = Status.STATUS_DB_UNSUPPORTED

    async def connect_and_setup_db(self, db: DB):
        "Connect to the database and perform setup tasks"
        App.db = db
        db.configure_decimal_context(decimal.DefaultContext)
        await check_db_schema()
        LOG.debug("DB ready")
        return db

    def import_db_by_name(self, db_name: str, db_class_name: str) -> type[DB]:
        "Import the DB class based on the resolved module name"
        LOG.debug(f"Importing DB class '{db_class_name}' from module '{db_name}'")
        module = importlib.import_module(f"database.dbms.{db_name.lower()}")

        db_class = getattr(module, db_class_name)
        return db_class


def get_db():
    "Create a DB connection"
    return DBManager(App).get_db()


log_exit(LOG)
