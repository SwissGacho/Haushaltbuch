"""Test suite for MySQL attachement"""

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


def restore_sys_modules_aiomysql(module=None):
    # print("====================== restoring sys.modules['aiomysql']")
    if module:
        sys.modules["aiomysql"] = module
    else:
        del sys.modules["aiomysql"]


def setUpModule():
    unittest.addModuleCleanup(
        restore_sys_modules_aiomysql, module=sys.modules.get("aiomysql")
    )
    # print("====================== patch sys.modules['aiomysql']")
    sys.modules["aiomysql"] = types.ModuleType("aiomysql")
    if sys.modules.get("database.dbms.mysql"):
        importlib.reload(sys.modules.get("database.dbms.mysql"))
    else:
        import database.dbms.mysql


import database.dbms.db_base


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

    def test_002_MySQLDB_no_lib(self):
        save_aiomysql = sys.modules["aiomysql"]
        sys.modules["aiomysql"] = None
        importlib.reload(sys.modules.get("database.dbms.mysql"))
        with self.assertRaises(ModuleNotFoundError):
            database.dbms.mysql.MySQLDB(**self.db_cfg)
        sys.modules["aiomysql"] = save_aiomysql
        importlib.reload(sys.modules.get("database.dbms.mysql"))

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

    async def test_101_connect(self):
        mock_mysql_connect = AsyncMock(return_value="mock_con")
        sys.modules["aiomysql"].connect = mock_mysql_connect
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

    async def _201_execute(self, params=DEFAULT):
        sql = "ANY_SQL"
        mock_cur = AsyncMock()
        MockCursor = Mock(return_value=mock_cur)
        self.con._connection = AsyncMock()
        self.con._connection.cursor.return_value = "mock_cur"
        sys.modules["aiomysql"].DictCursor = "mock_dict_cursor"
        with (patch("database.dbms.mysql.MySQLCursor", MockCursor),):

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
