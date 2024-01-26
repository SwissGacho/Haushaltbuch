""" Manage connection to the database
"""


import asyncio
from contextlib import asynccontextmanager
import aiomysql

from core.app import App
from core.status import Status
from core.config import Config
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DB:
    "Connection to the DB"

    def __init__(self) -> None:
        self._connection = None

    @property
    def connection(self):
        "DB connection"
        return self._connection

    @connection.setter
    def connection(self, con):
        self._connection = con

    def __repr__(self) -> str:
        return f"connection: {self._connection}"

    async def check(self):
        "Check DB for valid schema"
        cur = await self._connection.cursor()
        num_tables = await cur.execute(
            f"""SELECT table_name FROM information_schema.tables 
                WHERE table_schema = '{self._connection.db}'"""
        )
        tables = {t[0] for t in await cur.fetchall()}

        await self._connection.commit()
        await cur.close()


@asynccontextmanager
async def get_db():
    "Create a DB connection"
    if App.status == Status.STATUS_DB_CFG:
        LOG.debug(f"DB configuration: {App.configuration[Config.CONFIG_DB.value]=}")
        db.connection = await aiomysql.connect(
            **App.configuration[Config.CONFIG_DB.value]
        )
        try:
            await db.check()
            LOG.debug("DB connected")
            yield db
        finally:
            db.connection.close()
    else:
        LOG.warning("No DB configuration available")
    yield None


db = DB()

# LOG.debug("module imported")
