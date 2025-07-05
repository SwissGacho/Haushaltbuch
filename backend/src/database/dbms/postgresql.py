"""Connection to PostgreSQL database using psycopg"""

from database.dbms.db_base import DB, Connection, Cursor
from core.app_logging import getLogger

LOG = getLogger(__name__)
try:
    import psycopg
except ModuleNotFoundError as err:
    AIOPOSTGRES_IMPORT_ERROR = err
else:
    AIOPOSTGRES_IMPORT_ERROR = None


def get_db(db_type: str = None, **cfg) -> DB:
    """Get a DB instance based on the db_type"""
    # LOG.debug(f"postgresql.get_db({db_type=}, {cfg=})")
    if db_type == "PostgreSQL":
        return PostgreSQLDB(**cfg)


class PostgreSQLDB(DB):
    def __init__(self, **cfg) -> None:
        if AIOPOSTGRES_IMPORT_ERROR:
            raise ModuleNotFoundError(f"Import error: {AIOPOSTGRES_IMPORT_ERROR}")
        super().__init__(**cfg)
