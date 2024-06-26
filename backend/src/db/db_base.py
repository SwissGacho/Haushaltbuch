""" Base class for DB connections """

# from persistance.business_object_base import BO_Base
from db.sqlfactory import SQLFactory
from db.sqlexecutable import SQL, SQLTemplate
from db.sqlexpression import SQLColumnDefinition
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DB:
    "application Data Base"

    def __init__(self, **cfg) -> None:
        self._cfg = cfg
        self._connections = set()

    @property
    def sqlFactory(self):
        "DB specific SQL factory"
        raise NotImplementedError("sqlFactory not defined on base class")

    def sql(self, query: SQL, **kwargs) -> str:
        "return the DB specific SQL"
        # LOG.debug(f"{query=}, {kwargs=}, query is callable:{callable(query)} ")
        if callable(query):
            return query(self, **kwargs)
        elif isinstance(query.value, str):
            return query.value
        raise ValueError(f"value of {query} not defined")

    def check_column(self, col, attr, tab):
        "check compatibility of a DB column with a business object attribute"
        # LOG.debug(f"DB.check_column({col=}, {attr=})")
        attr_sql = SQL().sql_factory.get_sql_class(SQLColumnDefinition)(*attr).sql()
        if col is None:
            LOG.error(
                f"column '{attr[0]}' in DB table '{tab}' is undefined in the DB instead of '{attr_sql}'"
            )
            return False
        if col != attr_sql:
            LOG.error(
                f"column '{attr[0]}' in DB table '{tab}' is defined '{col}' in the DB instead of '{attr_sql}'"
            )
            return False
        return True

    async def check_table(self, obj: "BOBase"):
        "check compatibility of a DB table with a business object"
        LOG.debug(f"Checking table {obj.table}")
        tab_info = {
            c["column_name"]: " ".join(
                [c["column_name"], c["column_type"], c["constraint"]]
            )
            for c in await (
                await SQL().script(SQLTemplate.TABLEINFO, table=obj.table).execute()
            ).fetchall()
        }
        ok = True
        for attr in obj.attribute_descriptions():
            ok = self.check_column(tab_info.get(attr[0]), attr, obj.table) and ok
        return ok

    async def connect(self):
        "Open a connection and return the Connection instance"
        raise ConnectionError("Called from DB base class.")

    async def execute(self, query: str, params=None, close=False, commit=False):
        """Open a connection, execute a query and return the Cursor instance.
        If 'close'=True close connection after fetching all rows"""
        # LOG.debug(f"execute: {query=}, {params=}, {close=}, {commit=}")
        return await (await self.connect()).execute(
            query=query, params=params, close=close, commit=commit
        )

    async def close(self):
        "close all activities"
        for con in [c for c in self._connections]:
            await con.close()


class Connection:
    "Connection to the DB"

    def __init__(self, db_obj: DB, commit=False, **cfg) -> None:
        self._cfg = cfg
        self._connection = None
        self._db = db_obj
        self._db._connections.add(self)
        self._commit = commit

    async def connect(self):
        "Open a connection and return the Connection instance"
        raise ConnectionError("Called from DB base class.")

    async def close(self):
        "close the connection"
        if self._connection:
            if self._commit:
                await self.commit()
            # LOG.debug("close connection")
            await self._connection.close()
            self._db._connections.remove(self)
            self._connection = None

    @property
    def connection(self):
        "'real' DB connection"
        return self._connection

    async def execute(self, query: str, params=None, close=False, commit=False):
        """execute an SQL statement and return the Cursor instance.
        If 'close'=True close connection after fetching all rows"""
        raise ConnectionError("Called from DB base class.")

    async def commit(self):
        "commit current transaction"
        # LOG.debug("commit connection")
        await self._connection.commit()

    def __repr__(self) -> str:
        return f"connection: {self._connection}"


class Cursor:
    "query cursor"

    def __init__(self, cur=None, con=None, close=False) -> None:
        self._cursor = cur
        self._connection = con
        self._rowcount = None
        self._close = close

    async def execute(self, query: str, params=None, close=False):
        """execute an SQL statement and return the Cursor instance (self).
        If 'close'=True close connection after fetching all rows
        If 'close'=1 close connection after fetching one row
        If 'close'=0 close connection immediatly (used for stetemants w/o result)"""
        raise ConnectionError("Called from DB base class.")

    @property
    async def rowcount(self):
        return self._rowcount

    async def fetchone(self, commit=False):
        "fetch the next row"
        result = await self._cursor.fetchone()
        if self._close == 1:
            if commit:
                self._connection._commit = commit
            await self.close()
            await self._connection.close()
        return result

    async def fetchall(self, commit=False):
        "fetch all remaining rows from cursor"
        result = await self._cursor.fetchall()
        if self._close:
            if commit:
                self._connection._commit = commit
            await self.close()
            await self._connection.close()
        return result

    async def __aiter__(self):
        "row generator to support the iterator protocol"
        while col := await self.fetchone() is not None:
            yield col
        if self._close:
            await self._connection.close()
        raise StopIteration

    async def close(self):
        "close the cursor"
        # LOG.debug("close cursor")
        if self._cursor:
            await self._cursor.close()
            self._cursor = None


LOG.debug(f"module {__name__} initialized")
