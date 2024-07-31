""" Connection to SQLit DB using aiosqlite """

import datetime
from pathlib import Path
import json
import re

from core.exceptions import OperationalError
from core.configuration.config import Config
from core.app_logging import getLogger
from database.db_base import DB, Connection, Cursor
from database.sqlexecutable import SQL, SQLTemplate, SQLScript
from database.sqlexpression import SQLColumnDefinition
from database.sqlfactory import SQLFactory
from persistance.bo_descriptors import BOColumnFlag, BOBaseBase

LOG = getLogger(__name__)
try:
    import aiosqlite
    import sqlite3
except ModuleNotFoundError as err:
    AIOSQLITE_IMPORT_ERROR = err
    sqlite3 = None
else:
    AIOSQLITE_IMPORT_ERROR = None


class SQLiteSQLFactory(SQLFactory):

    @classmethod
    def get_sql_class(cls, sql_cls: type):
        # LOG.debug(f"SQLiteSQLFactory.get_sql_class({sql_cls=})")
        for sqlite_class in [SQLiteColumnDefinition, SQLiteScript]:
            if sql_cls.__name__ in [b.__name__ for b in sqlite_class.__bases__]:
                return sqlite_class
        return super().get_sql_class(sql_cls)


SQLITE_JSON_TYPE = "JSON"


class SQLiteColumnDefinition(SQLColumnDefinition):

    type_map = {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
        datetime.datetime: "TEXT",
        dict: SQLITE_JSON_TYPE,
        list: SQLITE_JSON_TYPE,
        BOBaseBase: "INTEGER",
    }
    constraint_map = {
        BOColumnFlag.BOC_NONE: "",
        BOColumnFlag.BOC_NOT_NULL: "NOT NULL",
        BOColumnFlag.BOC_UNIQUE: "UNIQUE",
        BOColumnFlag.BOC_PK: "PRIMARY KEY",
        BOColumnFlag.BOC_PK_INC: "PRIMARY KEY AUTOINCREMENT",
        BOColumnFlag.BOC_FK: "REFERENCES {relation}",
        BOColumnFlag.BOC_DEFAULT: "DEFAULT",
        BOColumnFlag.BOC_DEFAULT_CURR: "DEFAULT CURRENT_TIMESTAMP",
        BOColumnFlag.BOC_INC: "not available ! @%?°",
        BOColumnFlag.BOC_CURRENT_TS: "not available ! @%?°",
    }


def _adapt_dict(value: dict) -> str:
    return json.dumps(value, separators=(",", ":"))


def _adapt_list(value: list) -> str:
    return json.dumps(value, separators=(",", ":"))


def _convert_json(value: bytes) -> dict | list:
    return json.loads(value)


if sqlite3:
    sqlite3.register_adapter(dict, _adapt_dict)
    sqlite3.register_adapter(list, _adapt_list)
    sqlite3.register_converter(SQLITE_JSON_TYPE, _convert_json)


class SQLiteScript(SQLScript):
    sql_templates = {
        # # # # SQL statement returning a result set with info on DB table 'table' with the following columns:
        # # # # column_name:    name of table column
        # # # # column_type:    data type of column
        # # # # pk:             primary key constraint: 0=none; 1=PK,no auto inc; 2=PK,auto inc
        # # # # dflt_value:     default value
        # # # SQLTemplate.TABLEINFO: """ SELECT
        # # #                             column_name
        # # #                             ,column_type
        # # #                             ,CONCAT(
        # # #                                 CASE WHEN pk=0 THEN ''
        # # #                                     WHEN autoinc=0 THEN 'PRIMARY KEY'
        # # #                                     ELSE 'PRIMARY KEY AUTOINCREMENT'
        # # #                                     END
        # # #                                 ,CASE WHEN LENGTH(dflt_value) IS NULL THEN ''
        # # #                                     ELSE CONCAT('DEFAULT ',dflt_value)
        # # #                                     END
        # # #                             ) AS "constraint"
        # # #                         FROM (SELECT name AS column_name, type AS column_type, pk, dflt_value
        # # #                                 FROM pragma_table_info('{table}')) c
        # # #                                 FULL OUTER JOIN (SELECT INSTR(sql,'PRIMARY KEY AUTOINCREMENT')>0 AS autoinc
        # # #                                                 FROM sqlite_master WHERE type='table' AND name = '{table}') s
        # # #                     """,
        # SQL statement returning list of tables
        SQLTemplate.TABLELIST: """ SELECT name as table_name FROM sqlite_master
                                    WHERE type = 'table' and substr(name,1,7) <> 'sqlite_'
                                """,
        SQLTemplate.TABLESQL: """SELECT sql FROM sqlite_master
                                WHERE type='table' AND name = '{table}'
                            """,
    }


class SQLiteDB(DB):
    def __init__(self, **cfg) -> None:
        if AIOSQLITE_IMPORT_ERROR:
            raise ModuleNotFoundError(f"Import error: {AIOSQLITE_IMPORT_ERROR}")
        super().__init__(**cfg)

    @property
    def sql_factory(self):
        return SQLiteSQLFactory

    async def _get_table_info(self, table_name: str) -> dict[str, str]:
        sql_text = (
            await (
                await SQL()
                .script(SQLTemplate.TABLESQL, table=table_name)
                .execute(close=1)
            ).fetchone()
        )["sql"]
        match = re.search(r"\(([^\)]*)\)", sql_text)
        info = {
            col.split(" ")[0]: col for col in [s.strip() for s in match[1].split(",")]
        }
        # LOG.debug(f"SQLiteDB._get_table_info({table_name=}) -> {info}")
        return info

    async def connect(self):
        "Open a connection"
        return await SQLiteConnection(db_obj=self, **self._cfg).connect()


class SQLiteConnection(Connection):
    async def connect(self):
        def row_factory(cursor, row):
            fields = [column[0] for column in cursor.description]
            return {key: value for key, value in zip(fields, row)}

        db_path = Path(self._cfg[Config.CONFIG_DBFILE])
        # LOG.debug(f"Connecting to {db_path=}")
        if not db_path.parent.exists():
            LOG.info(f"Create missing directory '{db_path.parent}' for SQLite DB.")
            db_path.parent.mkdir(parents=True)
        if not db_path.parent.is_dir():
            raise FileExistsError(
                f"Path containing SQLite DB exists and is not a directory: {db_path.parent}"
            )
        self._connection = await aiosqlite.connect(
            database=db_path, detect_types=sqlite3.PARSE_DECLTYPES
        )
        await (await self._connection.execute("PRAGMA foreign_keys = ON")).close()
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
            # LOG.debug(f"Executing: {query=}, {params=}, {close=}")
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
