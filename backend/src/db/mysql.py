""" Connection to MySQL DB using aiomysql """

from db.db_base import DB, Connection, Cursor, SQL
from core.app_logging import getLogger

LOG = getLogger(__name__)

try:
    import aiomysql
except ModuleNotFoundError:
    AIOMYSQL_IMPORTED = False
else:
    AIOMYSQL_IMPORTED = True


class MySQLDB(DB):
    def __init__(self, host, db, user, password) -> None:
        if not AIOMYSQL_IMPORTED:
            raise ModuleNotFoundError("No module named 'aiomysql'")
        self._host = host
        self._db = db
        self._user = user
        self._password = password
        super().__init__()

    async def connect(self):
        "Open a connection"
        con = MySQLConnection(
            self,
            await aiomysql.connect(
                host=self._host, db=self._db, user=self._user, password=self._password
            ),
        )
        return con

    def sql(self, sql: SQL, **kwargs) -> str:
        if sql == SQL.TABLE_LIST:
            return f""" SELECT table_name FROM information_schema.tables 
                        WHERE table_schema = '{self._db}'
                    """
        else:
            return super().sql(sql=sql, **kwargs)


class MySQLConnection(Connection):

    async def execute(self, sql: str):
        "execute an SQL statement and return a cursor"
        cur = MySQLCursor(cur=await self._connection.cursor(), con=self)
        await cur.execute(sql)
        return cur


class MySQLCursor(Cursor):

    async def execute(self, sql: str):
        self._rowcount = await self._cursor.execute(sql)
