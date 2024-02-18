""" Test suite for SQLite attachement """

import sys
import types
import importlib

import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call
from contextlib import asynccontextmanager


def restore_sys_modules_aiosqlite(module=None):
    # print("====================== restoring sys.modules['aiosqlite']")
    if module:
        sys.modules["aiosqlite"] = module
    else:
        del sys.modules["aiosqlite"]


def setUpModule():
    unittest.addModuleCleanup(
        restore_sys_modules_aiosqlite, module=sys.modules.get("aiosqlite")
    )
    # print("====================== patch sys.modules['aiosqlite']")
    sys.modules["aiosqlite"] = types.ModuleType("aiosqlite")
    if sys.modules.get("db.sqlite"):
        importlib.reload(sys.modules.get("db.sqlite"))
    else:
        import db.sqlite


import db.db_base
import db.sql


class TestSQLiteDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {"file": "sqlite_file.db"}
        self.db = db.sqlite.SQLiteDB(**self.db_cfg)
        return super().setUp()

    def test_001_SQLiteDB(self):
        self.assertEqual(self.db._cfg, self.db_cfg)

    def test_002_SQLiteDB_no_lib(self):
        save_aioaiosqlite = sys.modules["aiosqlite"]
        sys.modules["aiosqlite"] = None
        importlib.reload(sys.modules.get("db.sqlite"))
        with self.assertRaises(ModuleNotFoundError):
            db.sqlite.SQLiteDB(**self.db_cfg)
        sys.modules["aiosqlite"] = save_aioaiosqlite
        importlib.reload(sys.modules.get("db.sqlite"))

    async def test_101_connect(self):
        mock_con_obj = AsyncMock()
        mock_con_obj.connect = AsyncMock(return_value=mock_con_obj)
        Mock_Connection = Mock(return_value=mock_con_obj)
        with (patch("db.sqlite.SQLiteConnection", Mock_Connection),):
            reply = await self.db.connect()
            self.assertEqual(reply, mock_con_obj)
            Mock_Connection.assert_called_once_with(db_obj=self.db, **self.db_cfg)
            mock_con_obj.connect.assert_awaited_once_with()

    def test_201_sql_table_list(self):
        reply = self.db.sql(db.sql.SQL.TABLE_LIST)
        re = "^ *SELECT.*FROM sqlite_master.*'table' *$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_202_sql_any_other(self):
        params = {"par1": ["el1", "el2"], "par2": "val"}
        mock_super = Mock(return_value="mock_sql")
        with patch("db.db_base.DB.sql", mock_super):
            reply = self.db.sql("ANY", **params)
            self.assertEqual(reply, mock_super.return_value)
            mock_super.assert_called_once_with(query="ANY", **params)


class TestSQLiteConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_db = Mock()
        self.mock_con = AsyncMock()
        self.db_cfg = {"file": "sqlite_file.db"}
        self.con = db.sqlite.SQLiteConnection(self.mock_db, **self.db_cfg)
        return super().setUp()

    def test_001_connection(self):
        self.assertDictEqual(self.con._cfg, self.db_cfg)

    async def test_101_connect(self):
        mock_sqlite_connect = AsyncMock(return_value="mock_con")
        sys.modules["aiosqlite"].connect = mock_sqlite_connect
        reply = await self.con.connect()
        self.assertEqual(reply, self.con)
        mock_sqlite_connect.assert_awaited_once_with(database=self.db_cfg["file"])

    async def test_201_execute(self):
        sql = "ANY_SQL"
        mock_cur = AsyncMock()
        MockCursor = Mock(return_value=mock_cur)
        self.con._connection = AsyncMock()
        self.con._connection.cursor.return_value = "mock_cur"
        with (patch("db.sqlite.SQLiteCursor", MockCursor),):
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


class TestSQLiteCursor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_con = Mock()
        self.mock_cur = AsyncMock()
        self.cur = db.sqlite.SQLiteCursor(self.mock_cur, self.mock_con)
        return super().setUp()

    async def test_101_execute(self):
        sql = "ANY_SQL"
        self.cur._last_sql = "PREV_SQL"
        self.mock_cur.rowcount = 99
        self.cur._rowcount = 0
        await self.cur.execute(sql)
        self.assertEqual(self.cur._last_sql, sql)
        self.mock_cur.execute.assert_awaited_once_with(sql)
        self.assertEqual(self.cur._rowcount, 99)

    async def test_201_rowcount_get_11(self):
        self.cur._rowcount = 11
        mock_sqlite_con_execute = AsyncMock(spec_async_context_manager())
        self.mock_con._connection.execute.return_value = mock_sqlite_con_execute
        reply = await self.cur.rowcount
        self.assertEqual(reply, 11)
        m = AsyncMock()
        m.assert_not_awaited
        self.mock_con._connection.execute.assert_not_called()
        mock_sqlite_con_execute.__aenter__.assert_not_awaited()
        mock_sqlite_con_execute.__aexit__.assert_not_awaited()

    async def test_201_rowcount_get_minus_1(self):
        self.cur._rowcount = -1
        self.cur._last_sql = "PREV_SQL"
        mock_sqlite_con_execute = AsyncMock(spec_async_context_manager())
        self.mock_con._connection.execute.return_value = mock_sqlite_con_execute
        mock_subcur = AsyncMock()
        mock_sqlite_con_execute.__aenter__.return_value = mock_subcur
        mock_subcur.fetchone.return_value = (22, "mick")
        reply = await self.cur.rowcount
        self.assertEqual(reply, 22)
        self.mock_con._connection.execute.assert_called_once_with(
            f"SELECT COUNT(*) FROM ({self.cur._last_sql})"
        )
        mock_sqlite_con_execute.__aenter__.assert_awaited_once_with()
        mock_sqlite_con_execute.__aexit__.assert_awaited_once_with(None, None, None)
        mock_subcur.fetchone.assert_awaited_once_with()
