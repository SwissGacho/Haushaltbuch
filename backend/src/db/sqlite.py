""" Connection to SQLit DB using aiosqlite """

import datetime

from core.exceptions import OperationalError
from core.config import Config
from core.app_logging import getLogger
from db.db_base import DB, Connection, Cursor
from backend.src.db.sqlexecutable import SQL
from backend.src.db.sqlexpression import SQLColumnDefinition
from backend.src.db.sqlfactory import SQLFactory

LOG = getLogger(__name__)
try:
    import aiosqlite
    import sqlite3
except ModuleNotFoundError as err:
    AIOSQLITE_IMPORT_ERROR = err
else:
    AIOSQLITE_IMPORT_ERROR = None


class SQLiteColumnDefinition(SQLColumnDefinition):

    type_map = {int: "INTEGER", float: "REAL", str: "TEXT", datetime.datetime: "TEXT"}


class SQLiteSQLFactory(SQLFactory):

    def get_sql_class(self, sql_cls: type):
        for sqlite_class in [SQLiteColumnDefinition]:
            if sql_cls.__name__ in [b.__name__ for b in sqlite_class.__bases__]:
                return sqlite_class
        return super().get_sql_class(sql_cls)


class SQLiteDB(DB):
    def __init__(self, **cfg) -> None:
        if AIOSQLITE_IMPORT_ERROR:
            raise ModuleNotFoundError(f"Import error: {AIOSQLITE_IMPORT_ERROR}")
        super().__init__(**cfg)

    @property
    def sql_factory(self):
        return SQLiteSQLFactory

    async def connect(self):
        "Open a connection"
        return await SQLiteConnection(db_obj=self, **self._cfg).connect()

    def sql(self, query: SQL, **kwargs) -> str:
        if query == SQL.TABLE_LIST:
            return f""" SELECT name as table_name FROM sqlite_master
                        WHERE type = 'table' and substr(name,1,7) <> 'sqlite_'
                    """
        elif query == SQL.TABLE_INFO:
            return f""" SELECT column_name, column_type, CASE WHEN pk=0 THEN 0 WHEN autoinc=0 THEN 1 ELSE 2 END AS pk,dflt_value
                        FROM (SELECT name AS column_name, type AS column_type, pk, dflt_value FROM pragma_table_info('{kwargs['table']}')) c
                        FULL OUTER JOIN (SELECT INSTR(sql,'PRIMARY KEY AUTOINCREMENT')>0 AS autoinc
                        FROM sqlite_master WHERE type='table' AND name = '{kwargs['table']}') s
                    """
        elif query == SQL.CREATE_TABLE_COLUMN:
            column = kwargs.get("column")
            col_def = [column[0]]
            if column[1] == int:
                col_def.append("INTEGER")
            if column[1] == str:
                col_def.append("TEXT")
            if column[1] == datetime.datetime:
                col_def.append("DATETIME")
            if column[1] == datetime.date:
                col_def.append("DATE")
            if len(column) > 2:
                if column[2] == "pkinc":
                    col_def.append("PRIMARY KEY AUTOINCREMENT")
                if column[2] == "pk":
                    col_def.append("PRIMARY KEY")
                if column[2] == "dt":
                    col_def.append("DEFAULT CURRENT_TIMESTAMP")
            return " ".join(col_def)
        else:
            return super().sql(query=query, **kwargs)


class SQLiteConnection(Connection):
    async def connect(self):
        def row_factory(cursor, row):
            fields = [column[0] for column in cursor.description]
            return {key: value for key, value in zip(fields, row)}

        self._connection = await aiosqlite.connect(
            database=self._cfg[Config.CONFIG_DB_FILE]
        )
        self._connection.row_factory = row_factory
        return self

    async def execute(self, query: str, params=None, close=False, commit=False):
        "execute an SQL statement and return a cursor"
        if commit:
            self._commit = commit
        cur = SQLiteCursor(cur=await self._connection.cursor(), con=self)
        await cur.execute(query, params=params, close=close)
        return cur


class SQLiteCursor(Cursor):

    async def execute(self, query: str, params=None, close=False):
        self._last_query = query
        self._close = close
        try:
            # LOG.debug(f"Executing: ({query}, {params}, {close=})")
            await self._cursor.execute(query, params)
            self._rowcount = self._cursor.rowcount
        except sqlite3.OperationalError as err:
            raise OperationalError(err)
        if close is 0:
            await self._connection.close()
            return None
        return self

    @property
    async def rowcount(self):
        if self._rowcount == -1:
            async with self._connection._connection.execute(
                f"SELECT COUNT(*) AS rowcount FROM ({self._last_query})"
            ) as sub_cur:
                self._rowcount = (await sub_cur.fetchone())["rowcount"]
        return self._rowcount
