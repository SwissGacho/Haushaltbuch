"""Base class for DB connections"""

from typing import Any, NoReturn
import re
import json

from core.app_logging import getLogger, log_exit
from core.base_objects import DBBaseClass, ConnectionBaseClass
from core.exceptions import OperationalError
from database.sql import SQL
from database.sql_statement import SQLTemplate
from database.sql_clause import SQLColumnDefinition

LOG = getLogger(__name__)


class DB(DBBaseClass):
    "application Data Base"

    def __init__(self, **cfg) -> None:
        self._cfg = cfg
        self._connections = set()

    @property
    def sql_factory(self):
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

    def check_column(self, tab, col, name, data_type, constraint, **pars):
        "check compatibility of a DB column with a business object attribute"
        # LOG.debug(
        #     f"DB.check_column({col=}, {name=}, {data_type=}, {constraint=}, {pars=})"
        # )
        attr_sql = SQLColumnDefinition(
            name, data_type, constraint, parent=SQL(), **pars
        ).get_query()
        if col is None:
            LOG.error(
                f"column '{name}' in DB table '{tab}' is undefined in the DB instead of '{attr_sql}'"
            )
            return False
        if col.strip() != attr_sql.strip():
            LOG.error(
                f"column '{name}' in DB table '{tab}' is defined '{col}' in the DB instead of '{attr_sql}'"
            )
            return False
        return True

    async def _get_table_info(self, table_name: str) -> dict[str, str]:
        LOG.debug(f"DB._get_table_info({table_name=})")
        async with SQL() as sql:
            table_info = await (
                await sql.script(SQLTemplate.TABLEINFO, table=table_name).execute()
            ).fetchall()
        return {
            c["column_name"]: " ".join(
                [c["column_name"], c["column_type"], c["constraint"], c["params"]]
            )
            for c in table_info
        }

    async def check_table(self, obj: "BOBase"):
        "check compatibility of a DB table with a business object"
        # LOG.debug(f"Checking table '{obj.table}'")
        tab_info = await self._get_table_info(obj.table)
        ok = True
        for name, data_type, constraint, pars in obj.attribute_descriptions():
            ok = (
                self.check_column(
                    obj.table, tab_info.get(name), name, data_type, constraint, **pars
                )
                and ok
            )
        LOG.debug(f"Check table '{obj.table}': {'OK'if ok else 'FAIL'}")
        return ok

    async def connect(self) -> "Connection":
        "Open a connection and return the Connection instance"
        raise ConnectionError("Called from DB base class.")

    async def execute(
        self,
        query: str,
        params=None,
        connection: "Connection" = None,
    ):
        """Open a connection, execute a query and return the Cursor instance.
        If 'close'=True close connection after fetching all rows"""
        # LOG.debug(f"DB.execute: {query=}, {params=}, {close=}, {commit=} {connection=}")
        return await (connection or await self.connect()).execute(
            query=query, params=params
        )

    async def close(self):
        "close all activities"
        if self._connections:
            LOG.warning(
                f"DB.close: closing {len(self._connections)} unclosed connections"
            )
            for con in [c for c in self._connections]:
                await con.close()


class Connection(ConnectionBaseClass):
    "Connection to the DB"

    def __init__(self, db_obj: DB, **cfg) -> None:
        self._cfg = cfg
        self._connection = None
        self._db = db_obj
        self._db._connections.add(self)
        # LOG.debug(f"++++++++++++++++++++++++++++++ {self._db._connections=}")

    async def connect(self):
        "Open a connection and return the Connection instance"
        raise ConnectionError("Called from DB base class.")

    async def begin(self):
        "begin a transaction"
        # LOG.debug("Connection.begin: begin transaction")
        try:
            await (await self.execute("BEGIN")).close()
        except Exception as exc:
            raise OperationalError(f"{exc} during transaction begin") from exc

    async def close(self):
        "close the connection"
        if self._connection:
            # LOG.debug("Connection.close: closing connection")
            try:
                await self._connection.close()
            except Exception as e:
                raise OperationalError(f"{e} during connection close") from e
            self._db._connections.remove(self)
            self._connection = None
            # LOG.debug(f"------------------------------- {self._db._connections=}")

    @property
    def connection(self):
        "'real' DB connection"
        return self._connection

    async def execute(self, query: str, params=None) -> "Cursor":
        """execute an SQL statement and return the Cursor instance.
        If 'close'=True close connection after fetching all rows"""
        raise ConnectionError("Called from DB base class.")

    async def commit(self):
        "commit current transaction"
        # LOG.debug("commit connection")
        try:
            await self._connection.commit()
        except Exception as e:
            raise OperationalError(f"{e} during commit") from e

    async def rollback(self):
        "rollback current transaction"
        # LOG.debug("rollback connection")
        try:
            await self._connection.rollback()
        except Exception as e:
            raise OperationalError(f"{e} during rollback") from e

    def __repr__(self) -> str:
        return f"connection: {self._connection}"


class Cursor:
    "query cursor"

    def __init__(self, cur=None, con=None) -> None:
        self._cursor = cur
        self._connection = con
        self._rowcount = None

    def convert_params_named_2_format(
        self, query, params, dump_json: bool = False
    ) -> tuple[str, tuple[Any, ...]]:
        param_order = []

        def replacer(match):
            key = match.group(1)
            if key not in params:
                raise ValueError(f"Fehlender Parameter: {key}")
            param = params[key]
            if dump_json and isinstance(param, (dict, list)):
                param = json.dumps(param)
            elif isinstance(param, str):
                param = param.replace("'", "''")
            param_order.append(param)
            return "%s"

        converted_query = re.sub(r":(\w+)", replacer, query)
        return converted_query, tuple(param_order)

    async def execute(self, query: str, params=None):
        """execute an SQL statement and return the Cursor instance (self)."""
        raise ConnectionError("Called from DB base class.")

    @property
    async def rowcount(self):
        return self._rowcount

    async def fetchone(self):
        "fetch the next row"
        # LOG.debug(f"Cursor.fetchone()")
        result = await self._cursor.fetchone()
        # LOG.debug(f"   Cursor.fetchone: {result=}")
        return result

    async def fetchall(self):
        "fetch all remaining rows from cursor"
        # LOG.debug(f"Cursor.fetchall()")
        result = await self._cursor.fetchall()
        return result

    async def __aiter__(self):
        "row generator to support the iterator protocol"
        while col := await self.fetchone() is not None:
            yield col
        raise StopIteration

    async def close(self):
        "close the cursor"
        # LOG.debug("Cursor.close: close cursor")
        if self._cursor:
            await self._cursor.close()
            self._cursor = None


log_exit(LOG)
