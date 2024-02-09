""" Connection to SQLit DB using aiosqlite """

import aiosqlite
from db.db_base import DB, Connection, Cursor, SQL
from core.app_logging import getLogger

LOG = getLogger(__name__)


class SQLiteDB(DB):
    def __init__(self, file) -> None:
        self._db_file = file
        super().__init__()

    async def connect(self):
        "Open a connection"
        con = SQLiteConnection(
            self,
            await aiosqlite.connect(database=self._db_file),
        )
        return con

    def sql(self, sql: SQL, **kwargs) -> str:
        if sql == SQL.TABLE_LIST:
            return f""" SELECT name FROM sqlite_master
                        WHERE type = 'table'
                    """
        else:
            return super().sql(sql=sql, **kwargs)


class SQLiteConnection(Connection):

    async def execute(self, sql: str):
        "execute an SQL statement and return a cursor"
        cur = SQLiteCursor(await self._connection.cursor(), self)
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
