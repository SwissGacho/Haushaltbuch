"""Test suite for MySQL attachement"""

import re
import sys
import types
import importlib

import unittest
from unittest.mock import (
    Mock,
    PropertyMock,
    MagicMock,
    AsyncMock,
    patch,
    call,
    ANY,
    DEFAULT,
    sentinel,
)
from contextlib import asynccontextmanager


def restore_sys_modules(name, module=None):
    # print(f"====================== restoring sys.modules['{name}'] -> {module}")
    if module:
        sys.modules[name] = module
    elif name in sys.modules:
        del sys.modules[name]


def setUpModule() -> None:
    def remove(mod):
        # print(f"----------------- remove {mod}")
        unittest.addModuleCleanup(
            restore_sys_modules, name=mod, module=sys.modules.get(mod)
        )
        if mod in sys.modules:
            del sys.modules[mod]

    remove("aiomysql")


from core.exceptions import ConfigurationError
import database.db_base
import database.sql_statement


class TestMySQLDB__init__(unittest.TestCase):
    def setUp(self) -> None:
        self.db_cfg = {
            "host": "mockhost",
            "db": "mockdb",
            "user": "mockuser",
        }
        return super().setUp()

    def test_001_MySQLDB(self):
        database.mysql.AIOMYSQL_IMPORT_ERROR = None
        with patch("database.db_base.DB.__init__") as mock_db_init:
            self.db = database.mysql.MySQLDB(**self.db_cfg)
            mock_db_init.assert_called_once_with(**self.db_cfg)

    def test_002_MySQLDB_no_lib(self):
        database.mysql.AIOMYSQL_IMPORT_ERROR = ModuleNotFoundError("Mock Error")
        with (
            self.assertRaises(ModuleNotFoundError),
            patch("database.db_base.DB.__init__") as mock_db_init,
        ):
            database.mysql.MySQLDB(**self.db_cfg)
        mock_db_init.assert_not_called()


class TestMySQLDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {
            "host": "mockhost",
            "db": "mockdb",
            "user": "mockuser",
            "password": "mockpw",
        }
        self.db = database.dbms.mysql.MySQLDB(**self.db_cfg)
        self.mockCur = AsyncMock(name="mockCursor")
        self.mockCur.fetchone = AsyncMock(return_value={"sql": "mock-foo"})
        self.sql = AsyncMock(spec=database.sql.SQL, name="mocksql")
        self.MockSQL = Mock(name="MockSQL", return_value=self.sql)
        self.sql.__aenter__ = AsyncMock(return_value=self.sql)
        self.sql.__aexit__ = AsyncMock()
        self.sql._get_db = Mock(return_value=self.db)
        self.sql.execute = AsyncMock(return_value=self.mockCur)
        self.sql.script = Mock(return_value=self.sql)
        return super().setUp()

    async def test_101_connect(self):
        mock_con_obj = AsyncMock()
        mock_con_obj.connect = AsyncMock(return_value=mock_con_obj)
        Mock_Connection = Mock(return_value=mock_con_obj)
        with (patch("database.dbms.mysql.MySQLConnection", Mock_Connection),):
            reply = await self.db.connect()
            self.assertEqual(reply, mock_con_obj)
            Mock_Connection.assert_called_once_with(db_obj=self.db, **self.db_cfg)
            mock_con_obj.connect.assert_awaited_once_with()

    async def test_201_get_table_info(self):
        mock_table = "mock_table"
        expected = {
            "mock-col-2": "mock-col-2 MOCK-TYP-2 MOCK-CONSTR-2",
            "mock-col-1": "mock-col-1 MOCK-TYP-1 MOCK-CONSTR-1",
        }
        self.mockCur.fetchall = AsyncMock(
            return_value=[
                {"name": "mock-col-1", "column_info": expected["mock-col-1"]},
                {"name": "mock-col-2", "column_info": expected["mock-col-2"]},
            ]
        )
        with patch("database.mysql.SQL", self.MockSQL):
            reply = await self.db._get_table_info(mock_table)
            self.sql.script.assert_called_once_with(
                database.sql_statement.SQLTemplate.TABLEINFO, table=mock_table
            )
            self.sql.execute.assert_awaited_once_with()
            self.mockCur.fetchall.assert_awaited_once_with()
            self.assertDictEqual(reply, expected)


class TestMySQLConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_db = Mock()
        self.mock_con = AsyncMock()
        self.db_cfg = {
            "host": "mock_host",
            "dbname": "mock_dbname",
            "dbuser": "mock_user",
            "password": "mock_pw",
        }
        self.con = database.dbms.mysql.MySQLConnection(self.mock_db, **self.db_cfg)
        return super().setUp()

    def test_001_connection(self):
        self.assertDictEqual(self.con._cfg, self.db_cfg)

    async def _100_connect(self, mock_db, mock_dbcfg="MySQL", checked=False):
        if mock_dbcfg == "MySQL":
            db_error = "MariaDB" in mock_db
        else:
            db_error = "MariaDB" not in mock_db
        mock_aioconnection = Mock(name="mock_aioconnection")
        mock_cursor = Mock(name="cursor")
        mock_aioconnection.execute = AsyncMock(return_value=mock_cursor)
        mock_cursor.close = AsyncMock()
        mock_mysql_connect = AsyncMock(
            name="mock_aioconnect", return_value=mock_aioconnection
        )
        mock_sql = AsyncMock(name="mock_sql")
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.script = Mock(return_value=mock_sql)
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_cursor.fetchone = AsyncMock(
            return_value={"version": f"mock_version: {mock_db}"}
        )
        database.mysql.MySQLConnection._version_checked = checked
        with (
            patch("database.mysql.aiomysql") as mock_mysql,
            patch("database.mysql.SQL", return_value=mock_sql) as Mock_SQL,
            patch("database.mysql.get_config_item", return_value=mock_dbcfg),
        ):
            mock_mysql.connect = mock_mysql_connect
            if db_error and not checked:
                with self.assertRaises(ConfigurationError):
                    await self.con.connect()
                return
            else:
                reply = await self.con.connect()
        self.assertEqual(reply, self.con)
        mock_mysql_connect.assert_awaited_once_with(
            **{
                "host": self.db_cfg["host"],
                "db": self.db_cfg["dbname"],
                "user": self.db_cfg["dbuser"],
                "password": self.db_cfg["password"],
            }
        )
        self.assertEqual(self.con._connection, mock_aioconnection)
        mock_aioconnection.execute.assert_not_awaited()
        mock_cursor.close.assert_not_awaited()
        if checked:
            Mock_SQL.assert_not_called()
            mock_sql.script.assert_not_called()
            mock_sql.execute.assert_not_awaited()
            mock_cursor.fetchone.assert_not_awaited()
            return
        Mock_SQL.assert_called_once_with(connection=self.con)
        mock_sql.__aenter__.assert_awaited_once_with()
        mock_sql.script.assert_called_once_with(
            database.sql_statement.SQLTemplate.DBVERSION
        )
        mock_sql.execute.assert_awaited_once_with()
        mock_cursor.fetchone.assert_awaited_once_with()

    async def test_101_connect_mariadb(self):
        await self._100_connect("MariaDB 0.0", "MariaDB")

    async def test_102_connect_mysql(self):
        await self._100_connect("MySQL 0.0")

    async def test_103_connect_mariadb_error(self):
        await self._100_connect("MariaDB 0.0")

    async def test_104_connect_mysql_error(self):
        await self._100_connect("MySQL 0.0", "MariaDB")

    async def test_105_connect_checked(self):
        await self._100_connect("MariaDB 0.0", checked=True)

    async def _201_execute(self, params=DEFAULT):
        sql = "ANY_SQL"
        mock_cur = AsyncMock()
        MockCursor = Mock(return_value=mock_cur)
        self.con._connection = AsyncMock()
        self.con._connection.cursor.return_value = "mock_cur"
        with (
            patch("database.mysql.MySQLCursor", MockCursor),
            patch("database.mysql.aiomysql.DictCursor", "mock_dict_cursor"),
        ):

            if params is DEFAULT:
                reply = await self.con.execute(sql)
            else:
                reply = await self.con.execute(sql, params=params)

            self.assertEqual(reply, mock_cur)
            MockCursor.assert_called_once_with(
                cur=self.con._connection.cursor.return_value,
                con=self.con,
            )
            self.con._connection.cursor.assert_called_once_with("mock_dict_cursor")
            mock_cur.execute.assert_awaited_once_with(sql, params=ANY)
            return mock_cur.execute

    async def test_201_execute(self):
        exec = await self._201_execute()
        exec.assert_awaited_once_with(ANY, params=None)
        exec = await self._201_execute(params=sentinel.PARAMS)
        exec.assert_awaited_once_with(ANY, params=sentinel.PARAMS)


@asynccontextmanager
async def spec_async_context_manager():
    yield


class TestMySQLCursor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_con = Mock()
        self.mock_aiocursor = AsyncMock()
        self.cur = database.dbms.mysql.MySQLCursor(self.mock_aiocursor, self.mock_con)
        return super().setUp()

    async def _101_execute(self, query, params=DEFAULT, sql=None, args=()):
        self.mock_aiocursor.reset_mock()
        self.mock_aiocursor.execute.return_value = 99
        self.cur._rowcount = 0
        if params is DEFAULT:
            reply = await self.cur.execute(query)
            sql = query
            params = {}
        elif params is not DEFAULT:
            reply = await self.cur.execute(query, params=params)
        self.assertEqual(reply, self.cur)
        self.mock_aiocursor.execute.assert_awaited_once_with(sql, args=args)
        self.assertEqual(
            await self.cur.rowcount, self.mock_aiocursor.execute.return_value
        )

    async def test_101_execute(self):
        await self._101_execute("ANY_SQL")
        await self._101_execute(
            "ANY :parm1 SQL :parm2",
            params={"parm1": "value1", "parm2": "value2"},
            sql="ANY %s SQL %s",
            args=("value1", "value2"),
        )
        await self._101_execute(
            "ANY :parm2 SQL :parm1",
            params={"parm1": "value1", "parm2": "value2"},
            sql="ANY %s SQL %s",
            args=("value2", "value1"),
        )
