""" Enumeration of SQL Snipets """

from enum import Enum, auto
from core.app_logging import getLogger

LOG = getLogger(__name__)


class SQL(Enum):
    TABLE_LIST = auto()
    CREATE_TABLE = (
        lambda obj, table, columns: f"CREATE TABLE {table.lower()} ({','.join(columns)})"
    )
    CREATE_TABLE_COLUMN = auto()

    def SELECT(obj, columns, table):
        sql = "SELECT "
        sql += ",".join(columns)
        sql += f" FROM {table}"
        return sql
