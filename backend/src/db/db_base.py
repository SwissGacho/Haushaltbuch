""" Base class for DB connections """

from enum import Enum, auto
from core.app_logging import getLogger

LOG = getLogger(__name__)


class SQL(Enum):
    TABLE_LIST = auto()

    def SELECT(columns, table):
        sql = "SELECT "
        sql += ",".join(columns)
        sql += f" FROM {table}"
        return sql


class DB:
    "application Data Base"

    def __init__(self) -> None:
        self._connections = set()

    def sql(self, sql: SQL, **kwargs) -> str:
        "return the DB specific SQL"
        if callable(sql):
            return sql(**kwargs)
        elif isinstance(sql.value, str):
            return sql.value
        raise ValueError(f"value of {sql} not defined")

    async def check(self):
        "Check DB for valid schema"
        con = await self.connect()
        cur = await con.execute(self.sql(SQL.TABLE_LIST))
        num_tables = await cur.rowcount
        LOG.debug(f"Found {num_tables} tables in DB:")
        tables = {t[0] for t in await cur.fetchall()}
        LOG.debug(f"{tables=}")

        await con.commit()
        await cur.close()
        await con.close()

    async def connect(self):
        "Open a connection"

    async def close(self):
        "close all activities"
        for con in [c for c in self._connections]:
            await con.close()


class Connection:
    "Connection to the DB"

    def __init__(self, db: DB, con=None) -> None:
        self._connection = con
        self._db = db
        self._db._connections.add(self)

    async def connect(self):
        "Open a connection"

    async def close(self):
        "close the connection"
        # LOG.debug("close connection")
        if self._connection:
            await self._connection.close()
            self._db._connections.remove(self)
            self._connection = None

    @property
    def connection(self):
        "DB connection"
        return self._connection

    @connection.setter
    def connection(self, con):
        self._connection = con

    async def execute(self, sql: str):
        "execute an SQL statement and return a cursor"

    async def commit(self):
        "commit current transaction"
        await self._connection.commit()

    def __repr__(self) -> str:
        return f"connection: {self._connection}"


class Cursor:
    "query cursor"

    def __init__(self, cur=None, con=None) -> None:
        self._cursor = cur
        self._connection = con
        self._rowcount = None

    async def execute(self, sql: str):
        "execute an SQL statement"

    @property
    async def rowcount(self):
        return self._rowcount

    async def fetchall(self):
        "fetch all remaining rows from cursor"
        return await self._cursor.fetchall()

    async def close(self):
        "close the cursor"
        # LOG.debug("close cursor")
        if self._cursor:
            await self._cursor.close()
            self._cursor = None
