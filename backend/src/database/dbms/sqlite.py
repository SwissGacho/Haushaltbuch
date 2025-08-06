"""Connection to SQLit DB using aiosqlite"""

from typing import Self
import datetime
from pathlib import Path
import json
import re

from core.exceptions import OperationalError
from core.configuration.config import Config
from core.app_logging import getLogger
from database.dbms.db_base import DB, Connection, Cursor
from database.sql import SQL
from database.sql_statement import SQLTemplate, SQLScript
from database.sql_clause import SQLColumnDefinition
from database.sql_factory import SQLFactory
from business_objects.bo_descriptors import BOColumnFlag, BOBaseBase
from business_objects.business_attribute_base import BaseFlag

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
SQLITE_BASEFLAG_TYPE = "FLAG"


class SQLiteColumnDefinition(SQLColumnDefinition):

    type_map = {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
        datetime.datetime: "TEXT",
        dict: SQLITE_JSON_TYPE,
        list: SQLITE_JSON_TYPE,
        BOBaseBase: "INTEGER",
        BaseFlag: SQLITE_BASEFLAG_TYPE,
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
        # BOColumnFlag.BOC_INC: "not available ! @%?°",
        # BOColumnFlag.BOC_CURRENT_TS: "not available ! @%?°",
    }


def _adapt_dict(value: dict) -> str:
    return json.dumps(value, separators=(",", ":"))


def _adapt_list(value: list) -> str:
    return json.dumps(value, separators=(",", ":"))


def _adapt_flag(value: BaseFlag) -> str:
    # LOG.debug(f"SQLite._adapt_flag({value=})")
    return str(value)


def _convert_json(value: bytes) -> dict | list:
    return json.loads(value)


if sqlite3:

    sqlite3.register_adapter(dict, _adapt_dict)
    sqlite3.register_adapter(list, _adapt_list)
    sqlite3.register_adapter(BaseFlag, _adapt_flag)
    sqlite3.register_converter(SQLITE_JSON_TYPE, _convert_json)

    # Register adapter and converter for all existing Flag subclasses
    for flag_type in list(BaseFlag.__subclasses__()):
        # LOG.debug(f"Registering adapter and converter for {flag_type=}")
        sqlite3.register_adapter(flag_type, _adapt_flag)

    # Adapt Flag.__init_subclass__ to register adapter and converter for new Flag subclasses
    flag_original_init_subclass = BaseFlag.__init_subclass__

    def flag_init_selfregistering_subclass(cls):
        flag_original_init_subclass()
        LOG.debug(f"Self-Registering adapter and converter for {cls=}")
        sqlite3.register_adapter(cls, _adapt_flag)

    BaseFlag.__init_subclass__ = classmethod(flag_init_selfregistering_subclass)


class SQLiteScript(SQLScript):
    sql_templates = {
        SQLTemplate.TABLELIST: """ SELECT name as table_name FROM sqlite_master
                                    WHERE type = 'table' and substr(name,1,7) <> 'sqlite_'
                                """,
        SQLTemplate.TABLESQL: """SELECT sql FROM sqlite_master
                                WHERE type='table' AND name = :table
                            """,
        SQLTemplate.VIEWLIST: """ SELECT name as view_name FROM sqlite_master
                                    WHERE type = 'view' and substr(name,1,7) <> 'sqlite_'
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
        # LOG.debug(f"SQLiteDB._get_table_info({table_name=})")
        async with SQL() as sql:
            sql_text = (
                await (
                    await sql.script(SQLTemplate.TABLESQL, table=table_name).execute()
                ).fetchone()
            )["sql"]
        match = re.search(r"\(([^\)]*)\)", sql_text)
        info = {
            col.split(" ")[0]: col for col in [s.strip() for s in match[1].split(",")]
        }
        # LOG.debug(f"SQLiteDB._get_table_info({table_name=}) -> {info}")
        return info

    async def connect(self) -> "SQLiteConnection":
        "Open a connection"
        return await SQLiteConnection(db_obj=self, **self._cfg).connect()


class SQLiteConnection(Connection):
    async def connect(self) -> Self:
        def row_factory(cursor, row):
            fields = [column[0] for column in cursor.description]
            return {key: value for key, value in zip(fields, row)}

        db_path = Path(self._cfg[Config.CONFIG_DBFILE])
        if not db_path.parent.exists():
            LOG.info(f"Create missing directory '{db_path.parent}' for SQLite DB.")
            db_path.parent.mkdir(parents=True)
        if not db_path.parent.is_dir():
            raise FileExistsError(
                f"Path containing SQLite DB exists and is not a directory: {db_path.parent}"
            )
        # LOG.debug(f"Connecting to SQLite DB: {db_path}")
        self._connection = await aiosqlite.connect(
            database=db_path, detect_types=sqlite3.PARSE_DECLTYPES
        )
        # LOG.debug(f"............................... {self._db._connections=}")
        await (await self._connection.execute("PRAGMA foreign_keys = ON")).close()
        self._connection.row_factory = row_factory
        return self

    async def execute(self, query: str, params=None) -> "SQLiteCursor":
        "execute an SQL statement and return a cursor"
        # LOG.debug(f"SQLiteConnection.execute({query=}, {params=})")
        cur = SQLiteCursor(cur=await self._connection.cursor(), con=self)
        await cur.execute(query, params=params)
        return cur


class SQLiteCursor(Cursor):

    async def execute(self, query: str, params={}):
        if sqlite3 is None:
            raise ImportError("sqlite3 module is not available.")
        self._last_query = query
        self._last_params = params
        try:
            # LOG.debug(f"SQLiteCursor.execute({query=}, {params=})")
            await self._cursor.execute(sql=query, parameters=params)
            self._rowcount = self._cursor.rowcount
        except sqlite3.OperationalError as exc:
            raise OperationalError(f"{exc} during SQL execution") from exc
        return self

    @property
    async def rowcount(self):
        if self._rowcount == -1:
            async with self._connection._connection.execute(
                sql=f"SELECT COUNT(*) AS rowcount FROM ({self._last_query})",
                parameters=self._last_params,
            ) as sub_cur:
                self._rowcount = (await sub_cur.fetchone())["rowcount"]
        return self._rowcount
