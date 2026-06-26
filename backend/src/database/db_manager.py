"""Manage connection to the database"""

from contextlib import asynccontextmanager
import importlib
from types import ModuleType
from typing import AsyncIterator, cast
import decimal

from core.app_logging import getLogger, log_exit, redact
from core.exceptions import ConfigurationError
from database.dbms.db_base import DB

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

    RECONNECT_ATTEMPTS: int = 3

    def __init__(self, app: type[App]):
        self._app = app
        self.db: DB | None = None

    _db_module: ModuleType | None = None

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
        db_module_name, db_class_name = DB_TYPE_MAP[db_type]
        module_imported = self._import_db_by_name(db_module_name)
        if not module_imported:
            raise ConfigurationError(f"Import failed for: {db_type}")
        try:
            db_class = getattr(self._db_module, db_class_name)
        except AttributeError as exc:
            LOG.error(
                f"DB class '{db_class_name}' not found in module '{db_module_name}'"
            )
            raise ConfigurationError(
                f"DB class '{db_class_name}' not found in module '{self._db_module=}'"
            ) from exc
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

        LOG.debug(f"DB configuration: {redact(db_config)=}, {db_type=}")

        db = db_class(decimal_context=decimal.DefaultContext, **db_config)

        # Post-connection configuration and schema check
        try:
            App.db = db
            await self._try_check_db_schema(db)
            LOG.debug("DB ready")
            yield db
        except (TypeError, ValueError) as exc:
            LOG.error(f"DB connection failed: {exc}")
            App.status = Status.STATUS_NO_DB
            yield None
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            LOG.warning(f"DB connection failed: {exc}")
            App.status = Status.STATUS_UNCONFIGURED
            yield None
            return
        finally:
            LOG.debug("DB disconnecting")
            await db.close()

    async def _try_check_db_schema(self, db: DB):
        "Try to check the database schema; on connection error, retry up to RECONNECT_ATTEMPTS times"
        attempts = 0
        while attempts < self.RECONNECT_ATTEMPTS:
            try:
                LOG.debug(f"Checking DB schema (attempt {attempts + 1})")
                await check_db_schema()
                return True
            except (TypeError, ValueError) as exc:
                LOG.error(f"DB schema check failed: {exc}")
                raise
            except Exception as exc:  # pylint: disable=broad-exception-caught
                attempts += 1
                if attempts >= self.RECONNECT_ATTEMPTS:
                    LOG.error(
                        f"DB schema check failed after {attempts} attempts: {exc}"
                    )
                    raise
                LOG.warning(
                    f"DB schema check failed (attempt {attempts}/{self.RECONNECT_ATTEMPTS}): {exc}"
                )

    def _set_db_unsupported(self):
        App.status = Status.STATUS_DB_UNSUPPORTED

    def _import_db_by_name(self, db_name: str) -> bool:
        "Import the DB class based on the resolved module name"
        LOG.debug(f"Importing db module '{db_name}'")
        try:
            module = importlib.import_module(f"database.dbms.{db_name.lower()}")
        except ModuleNotFoundError as exc:
            missing_module = exc.name or "<unknown>"
            LOG.error(
                f"Library '{missing_module}' could not be imported for {db_name=}"
            )
            LOG.error(f"Please install using 'pip install {missing_module}'")
            LOG.error(f"{exc}")
            return False
        self._db_module = module
        return True


def get_db():
    "Create a DB connection"
    return DBManager(App).get_db()


log_exit(LOG)
