"""Test suite for SQLite attachement"""

import re
import sys, types
import importlib
import pathlib

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
from datetime import datetime as dt, date


def restore_sys_modules(name, module=None):
    print(f"====================== restoring sys.modules['{name}'] -> {module}")
    if module:
        sys.modules[name] = module
    elif name in sys.modules:
        del sys.modules[name]


def setUpModule() -> None:
    def remove(mod):
        print(f"----------------- remove {mod}")
        unittest.addModuleCleanup(
            restore_sys_modules, name=mod, module=sys.modules.get(mod)
        )
        if mod in sys.modules:
            del sys.modules[mod]

    remove("aiosqlite")
    remove("sqlite3")


import database.db_base
import database.sql_statement


class TestSQLiteDB__init__(unittest.TestCase):
    def setUp(self) -> None:
        self.db_cfg = {"file": "mock_sqlite_file.db"}
        return super().setUp()

    def test_001_SQLiteDB(self):
        database.sqlite.AIOSQLITE_IMPORT_ERROR = None
        with patch("database.db_base.DB.__init__") as mock_db_init:
            self.db = database.sqlite.SQLiteDB(**self.db_cfg)
            mock_db_init.assert_called_once_with(**self.db_cfg)

    def test_002_SQLiteDB_no_lib(self):
        database.sqlite.AIOSQLITE_IMPORT_ERROR = ModuleNotFoundError("Mock Error")
        with (
            self.assertRaises(ModuleNotFoundError),
            patch("database.db_base.DB.__init__") as mock_db_init,
        ):
            database.sqlite.SQLiteDB(**self.db_cfg)
            mock_db_init.assert_not_called()


class TestSQLiteDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {"db": "SQLite", "file": "sqlite_file.db"}
        database.sqlite.AIOSQLITE_IMPORT_ERROR = None
        self.db = database.sqlite.SQLiteDB(**self.db_cfg)
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
        with (patch("database.sqlite.SQLiteConnection", Mock_Connection),):
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
        self.mockCur.fetchone = AsyncMock(
            return_value={
                "sql": f"mock-foo ({expected['mock-col-1']},"
                f"{expected['mock-col-2']})mock-bar"
            }
        )
        with patch("database.sqlite.SQL", self.MockSQL):
            reply = await self.db._get_table_info(mock_table)
            self.sql.script.assert_called_once_with(
                database.sql_statement.SQLTemplate.TABLESQL, table=mock_table
            )
            self.sql.execute.assert_awaited_once_with()
            self.mockCur.fetchone.assert_awaited_once_with()
            self.assertDictEqual(reply, expected)


class TestSQLiteConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_db = Mock()
        self.mock_con = AsyncMock()
        self.db_cfg = {"db": "SQLite", "file": "mock_sqlite_file.db"}
        self.con = database.sqlite.SQLiteConnection(self.mock_db, **self.db_cfg)
        return super().setUp()

    def test_001_connection(self):
        self.assertDictEqual(self.con._cfg, self.db_cfg)

    async def test_101_connect(self):
        mock_aioconnection = Mock(name="mock_aioconnection")
        mock_cursor = Mock(name="cursor")
        mock_aioconnection.execute = AsyncMock(return_value=mock_cursor)
        mock_cursor.close = AsyncMock()
        mock_sqlite_connect = AsyncMock(
            name="mock_aioconnect", return_value=mock_aioconnection
        )
        with patch("database.sqlite.aiosqlite") as mock_sqlite:
            mock_sqlite.connect = mock_sqlite_connect
            reply = await self.con.connect()
        self.assertEqual(reply, self.con)
        mock_sqlite_connect.assert_awaited_once_with(
            database=pathlib.Path(self.db_cfg["file"]), detect_types=1, autocommit=False
        )
        self.assertEqual(self.con._connection, mock_aioconnection)
        mock_aioconnection.execute.assert_awaited_once_with("PRAGMA foreign_keys = ON")
        mock_cursor.close.assert_awaited_once_with()

        # test rowfactory
        mock_cursor = Mock()
        mock_result = {"mock_1": "val_1", "mock_2": 2, "mock_3": "val_3"}
        mock_cursor.description = [(m,) for m in mock_result.keys()]
        mock_row = tuple(mock_result.values())
        result = mock_aioconnection.row_factory(mock_cursor, mock_row)
        self.assertEqual(result, mock_result)

    async def _201_execute(self, params=DEFAULT):
        sql = "ANY_SQL"
        mock_cur = AsyncMock()
        MockCursor = Mock(return_value=mock_cur)
        self.con._connection = AsyncMock()
        mock_aiocursor = "mock_cur"
        self.con._connection.cursor.return_value = mock_aiocursor
        self.con.commit = sentinel.COMMIT
        with (patch("database.sqlite.SQLiteCursor", MockCursor),):
            if params is DEFAULT:
                reply = await self.con.execute(sql)
            else:
                reply = await self.con.execute(sql, params=params)
            self.assertEqual(reply, mock_cur)
            MockCursor.assert_called_once_with(
                cur=self.con._connection.cursor.return_value,
                con=self.con,
            )
            self.con._connection.cursor.assert_called_once_with()
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


class TestSQLiteCursor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_con = Mock(name="mock_connection")
        self.mock_aiocursor = AsyncMock(name="mock_aiocursor")
        self.cur = database.sqlite.SQLiteCursor(self.mock_aiocursor, self.mock_con)
        self.mock_con_close = AsyncMock(name="mock_conclose")
        self.mock_con.close = self.mock_con_close
        return super().setUp()

    async def _101_execute(self, params=DEFAULT):
        query = "ANY_SQL"
        self.cur._last_query = "PREV_SQL"
        self.mock_aiocursor.reset_mock()
        self.mock_aiocursor.rowcount = 99
        self.cur._rowcount = 0
        if params is DEFAULT:
            reply = await self.cur.execute(query)
            params = {}
        elif params is not DEFAULT:
            reply = await self.cur.execute(query, params=params)
        self.assertEqual(reply, self.cur)
        self.assertEqual(self.cur._last_query, query)
        self.assertEqual(self.cur._rowcount, 99)
        self.mock_aiocursor.execute.assert_awaited_once_with(
            sql=query, parameters=params
        )

    async def test_101_execute(self):
        await self._101_execute()
        await self._101_execute(sentinel.PARAMS)

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
        _last_sql = "PREV_SQL"
        self.cur._last_query = _last_sql
        self.cur._last_params = {}
        mock_sqlite_con_execute = AsyncMock(spec_async_context_manager())
        self.mock_con._connection.execute.return_value = mock_sqlite_con_execute
        mock_subcur = AsyncMock()
        mock_sqlite_con_execute.__aenter__.return_value = mock_subcur
        mock_subcur.fetchone.return_value = {"rowcount": 22}
        reply = await self.cur.rowcount
        self.assertEqual(reply, 22)
        self.mock_con._connection.execute.assert_called_once_with(
            sql=f"SELECT COUNT(*) AS rowcount FROM ({_last_sql})", parameters={}
        )
        mock_sqlite_con_execute.__aenter__.assert_awaited_once_with()
        mock_sqlite_con_execute.__aexit__.assert_awaited_once_with(None, None, None)
        mock_subcur.fetchone.assert_awaited_once_with()
