""" Connection to SQLit DB using aiosqlite """

from db.db_base import DB, Connection, Cursor
from db.sql import SQL
from core.config import Config
from core.app_logging import getLogger

LOG = getLogger(__name__)

try:
    import aiosqlite
except ModuleNotFoundError:
    AIOSQLITE_IMPORTED = False
else:
    AIOSQLITE_IMPORTED = True


class SQLiteDB(DB):
    def __init__(self, **cfg) -> None:
        if not AIOSQLITE_IMPORTED:
            raise ModuleNotFoundError("No module named 'aiosqlite'")
        super().__init__(**cfg)

    async def connect(self):
        "Open a connection"
        return await SQLiteConnection(db_obj=self, **self._cfg).connect()

    def sql(self, query: SQL, **kwargs) -> str:
        if query == SQL.TABLE_LIST:
            return f""" SELECT name FROM sqlite_master
                        WHERE type = 'table'
                    """
        elif query == SQL.CREATE_TABLE_COLUMN:
            column = kwargs.get("column")
            col_def = [column[0]]
            if column[1] == int:
                col_def.append("INTEGER")
            if column[1] == str:
                col_def.append("TEXT")
            if len(column) > 2:
                if column[2] == "primary":
                    col_def.append("PRIMARY KEY")
            return " ".join(col_def)
        else:
            return super().sql(query=query, **kwargs)


class SQLiteConnection(Connection):
    async def connect(self):
        self._connection = await aiosqlite.connect(
            database=self._cfg[Config.CONFIG_DB_FILE]
        )
        return self

    async def execute(self, sql: str):
        "execute an SQL statement and return a cursor"
        cur = SQLiteCursor(cur=await self._connection.cursor(), con=self)
        await cur.execute(sql)
        return cur


class SQLiteCursor(Cursor):

    async def execute(self, sql: str):
        self._last_sql = sql
        await self._cursor.execute(sql)
        self._rowcount = self._cursor.rowcount

    @property
    async def rowcount(self):
        if self._rowcount == -1:
            async with self._connection._connection.execute(
                f"SELECT COUNT(*) FROM ({self._last_sql})"
            ) as sub_cur:
                self._rowcount = (await sub_cur.fetchone())[0]
        return self._rowcount
