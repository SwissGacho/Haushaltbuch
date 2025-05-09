"""Connection to MySQL DB using aiomysql"""

from database.db_base import DB, Connection, Cursor
from database.sql_factory import SQLFactory
from database.sql_statement import SQL
from core.configuration.config import Config
from core.app_logging import getLogger

LOG = getLogger(__name__)

try:
    import aiomysql
except ModuleNotFoundError as err:
    AIOMYSQL_IMPORT_ERROR = err
else:
    AIOMYSQL_IMPORT_ERROR = None


class MySQLDB(DB):
    def __init__(self, **cfg) -> None:
        if not AIOMYSQL_IMPORT_ERROR:
            raise ModuleNotFoundError(f"Import error: {err}")
        super().__init__(**cfg)

    @property
    def sql_factory(self):
        return SQLFactory

    async def connect(self):
        "Open a connection"
        return await MySQLConnection(db_obj=self, **self._cfg).connect()

    # def sql(self, query: SQL, **kwargs) -> str:
    #    if query == SQL.TABLE_LIST:
    #        return f""" SELECT table_name FROM information_schema.tables
    #                    WHERE table_schema = '{self._cfg['db']}'
    #                """
    #    else:
    #        return super().sql(query=query, **kwargs)*/


class MySQLConnection(Connection):
    async def connect(self):
        self._connection = await aiomysql.connect(
            host=self._cfg[Config.CONFIG_DBHOST],
            # db=self._cfg[Config.CONFIG_DB_DB],
            user=self._cfg[Config.CONFIG_DBUSER],
            password=self._cfg[Config.CONFIG_DBPW],
        )
        return self

    async def execute(self, sql: str):
        "execute an SQL statement and return a cursor"
        cur = MySQLCursor(cur=await self._connection.cursor(), con=self)
        await cur.execute(sql)
        return cur


class MySQLCursor(Cursor):

    async def execute(self, sql: str):
        self._rowcount = await self._cursor.execute(sql)
