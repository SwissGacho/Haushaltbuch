""" Test suite for MySQL attachement """

import sys
import types
import importlib

import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call
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
    if sys.modules.get("db.mysql"):
        importlib.reload(sys.modules.get("db.mysql"))
    else:
        import db.mysql


import db.db_base
import db.sql


@unittest.skip("MySQL module is not maintained currently")
class TestMySQLDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {
            "host": "mockhost",
            "db": "mockdb",
            "user": "mockuser",
            "password": "mockpw",
        }
        self.db = db.mysql.MySQLDB(**self.db_cfg)
        return super().setUp()

    def test_001_MySQLDB(self):
        self.assertDictEqual(self.db._cfg, self.db_cfg)

    def test_002_MySQLDB_no_lib(self):
        save_aiomysql = sys.modules["aiomysql"]
        sys.modules["aiomysql"] = None
        importlib.reload(sys.modules.get("db.mysql"))
        with self.assertRaises(ModuleNotFoundError):
            db.mysql.MySQLDB(**self.db_cfg)
        sys.modules["aiomysql"] = save_aiomysql
        importlib.reload(sys.modules.get("db.mysql"))

    async def test_101_connect(self):
        mock_con_obj = AsyncMock()
        mock_con_obj.connect = AsyncMock(return_value=mock_con_obj)
        Mock_Connection = Mock(return_value=mock_con_obj)
        with (patch("db.mysql.MySQLConnection", Mock_Connection),):
            reply = await self.db.connect()
            self.assertEqual(reply, mock_con_obj)
            Mock_Connection.assert_called_once_with(db_obj=self.db, **self.db_cfg)
            mock_con_obj.connect.assert_awaited_once_with()

    def test_201_sql_table_list(self):
        reply = self.db.sql(db.sql.SQL.TABLE_LIST)
        re = f"^ *SELECT.*FROM information_schema.tables.*'{self.db_cfg['db']}' *$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_202_sql_any_other(self):
        params = {"par1": ["el1", "el2"], "par2": "val"}
        mock_super = Mock(return_value="mock_sql")
        with patch("db.db_base.DB.sql", mock_super):
            reply = self.db.sql("ANY", **params)
            self.assertEqual(reply, mock_super.return_value)
            mock_super.assert_called_once_with(sql="ANY", **params)


class TestMySQLConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_db = Mock()
        self.mock_con = AsyncMock()
        self.db_cfg = {
            "host": "mock_host",
            "db": "mock_db",
            "user": "mock_user",
            "password": "mock_pw",
        }
        self.con = db.mysql.MySQLConnection(self.mock_db, **self.db_cfg)
        return super().setUp()

    def test_001_connection(self):
        self.assertDictEqual(self.con._cfg, self.db_cfg)

    async def test_101_connect(self):
        mock_mysql_connect = AsyncMock(return_value="mock_con")
        sys.modules["aiomysql"].connect = mock_mysql_connect
        reply = await self.con.connect()
        self.assertEqual(reply, self.con)
        mock_mysql_connect.assert_awaited_once_with(**self.db_cfg)

    async def test_201_execute(self):
        sql = "ANY_SQL"
        mock_cur = AsyncMock()
        MockCursor = Mock(return_value=mock_cur)
        self.con._connection = AsyncMock()
        self.con._connection.cursor.return_value = "mock_cur"
        with (patch("db.mysql.MySQLCursor", MockCursor),):
            reply = await self.con.execute(sql)
            self.assertEqual(reply, mock_cur)
            MockCursor.assert_called_once_with(
                cur=self.con._connection.cursor.return_value,
                con=self.con,
            )
            self.con._connection.cursor.assert_called_once_with()
            mock_cur.execute.assert_awaited_once_with(sql)


@asynccontextmanager
async def spec_async_context_manager():
    yield


class TestMySQLCursor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_con = Mock()
        self.mock_cur = AsyncMock()
        self.cur = db.mysql.MySQLCursor(self.mock_cur, self.mock_con)
        return super().setUp()

    async def test_101_execute(self):
        sql = "ANY_SQL"
        self.mock_cur.execute.return_value = 99
        self.cur._rowcount = 0
        await self.cur.execute(sql)
        self.mock_cur.execute.assert_awaited_once_with(sql)
        self.assertEqual(self.cur._rowcount, self.mock_cur.execute.return_value)
