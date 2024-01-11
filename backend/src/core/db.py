""" Manage connection to the database
"""

import asyncio
from contextlib import asynccontextmanager
import aiomysql


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
        cur = await self._connection.cursor(aiomysql.DictCursor)
        await cur.execute(
            f"""SELECT table_name FROM information_schema.tables 
                WHERE table_schema = '{self._connection.db}'"""
        )
        r = await cur.fetchall()
        print(f"{r=}, {len(r)=}")
        await cur.close()


@asynccontextmanager
async def get_db(db_cfg={}):
    "Create a DB connection"
    db.connection = await aiomysql.connect(**db_cfg)
    try:
        await db.check()
        yield db
    finally:
        db.connection.close()


db = DB()