""" Enumeration of SQL Snipets """

from enum import Enum, auto
from core.app_logging import getLogger

LOG = getLogger(__name__)


class SQL(Enum):
    TABLE_LIST = auto()
    TABLE_INFO = auto()
    CREATE_TABLE = (
        lambda obj, table, columns: f"""CREATE TABLE IF NOT EXISTS
            {table.lower()} ( {','.join(columns)} )"""
    )
    CREATE_TABLE_COLUMN = auto()

    def COUNT_ROWS(obj, table, conditions):
        sql = f"SELECT COUNT(*) AS Count FROM {table}"
        if conditions:
            sql += " WHERE "
            sql += " AND ".join([f"{k} = '{v}'" for k, v in conditions.items()])
        return sql

    def SELECT_ID_BY_CONDITION(obj, table, conditions=None):
        sql = f"SELECT id FROM {table}"
        if conditions == None:
            return sql
        sql += " WHERE "
        sql += " AND ".join([f"{k} = '{v}'" for k, v in conditions.items()])
        return sql

    def SELECT(obj, table, columns=None, id=None, newest=None):
        if not columns:
            columns = "*"
        if not isinstance(columns, list):
            columns = [columns]
        sql = "SELECT "
        sql += ",".join(columns)
        sql += f" FROM {table}"
        if id:
            sql += f" WHERE id = {id}"
        elif newest:
            sql += f" WHERE id = (SELECT max(id) FROM {table})"
        return sql

    def INSERT(obj, table, columns, returning=None):
        sql = f"INSERT INTO {table} ( "
        sql += " , ".join(columns.keys())
        sql += " ) VALUES ( "
        sql += " , ".join([str(v) for v in columns.values()])
        sql += " )"
        if returning:
            sql += f" RETURNING {', '.join(returning)}"
        return sql

    def INSERT_ARGS(obj, table, columns, returning=None):
        cols = ", ".join(columns.keys())
        placeholders = ", ".join(":" + key for key in columns.keys())
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        if returning:
            sql += f" RETURNING {', '.join(returning)}"
        return sql

    def UPDATE_ARGS(obj, table, columns):
        update_string = ", ".join([f"{k} = :{k}" for k in columns.keys()])
        return f"UPDATE {table} SET {update_string} WHERE id = :id"
