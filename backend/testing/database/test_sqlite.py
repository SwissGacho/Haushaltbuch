""" Test suite for SQLite attachement """

import sys, types
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
from datetime import datetime as dt, date


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

    remove("aiosqlite")
    remove("sqlite3")


import db.db_base
import db.sql


class TestSQLiteDB__init__(unittest.TestCase):
    def setUp(self) -> None:
        self.db_cfg = {"file": "mock_sqlite_file.db"}
        return super().setUp()

    def test_001_SQLiteDB(self):
        database.sqlite.AIOSQLITE_IMPORT_ERROR = None
        with patch("db.db_base.DB.__init__") as mock_db_init:
            self.db = database.sqlite.SQLiteDB(**self.db_cfg)
            mock_db_init.assert_called_once_with(**self.db_cfg)

    def test_002_SQLiteDB_no_lib(self):
        database.sqlite.AIOSQLITE_IMPORT_ERROR = ModuleNotFoundError("Mock Error")
        with (
            self.assertRaises(ModuleNotFoundError),
            patch("db.db_base.DB.__init__") as mock_db_init,
        ):
            database.sqlite.SQLiteDB(**self.db_cfg)
            mock_db_init.assert_not_called()


class TestSQLiteDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {"file": "sqlite_file.db"}
        database.sqlite.AIOSQLITE_IMPORT_ERROR = None
        self.db = database.sqlite.SQLiteDB(**self.db_cfg)
        return super().setUp()

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
        reply = self.db.sql(database.sql.SQL.TABLE_LIST)
        re = "^ *SELECT.*FROM sqlite_master.*'table'"
        re += ".*substr.*'sqlite_' *$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_202_sql_table_info(self):
        mock_table = "mock_table"
        reply = self.db.sql(database.sql.SQL.TABLE_INFO, table=mock_table)
        re = "^ *SELECT column_name, column_type,.*END AS pk,dflt_value"
        re += f".*WHERE type='table' AND name = '{mock_table}'.*$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_221_sql_create_table_anytype_column(self):
        mock_col_name = "mock_col_name"
        mock_column = (mock_col_name, "Mock")
        reply = self.db.sql(database.sql.SQL.CREATE_TABLE_COLUMN, column=mock_column)
        re = f"^{mock_col_name}$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_222_sql_create_table_PK_column(self):
        mock_col_name = "mock_col_name"
        mock_column = (mock_col_name, int, "pkinc")
        reply = self.db.sql(database.sql.SQL.CREATE_TABLE_COLUMN, column=mock_column)
        re = f"^{mock_col_name} INTEGER PRIMARY KEY AUTOINCREMENT$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_223_sql_create_table_int_column(self):
        mock_col_name = "mock_col_name"
        mock_column = (mock_col_name, int)
        reply = self.db.sql(database.sql.SQL.CREATE_TABLE_COLUMN, column=mock_column)
        re = f"^{mock_col_name} INTEGER$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_224_sql_create_table_str_column(self):
        mock_col_name = "mock_col_name"
        mock_column = (mock_col_name, str)
        reply = self.db.sql(database.sql.SQL.CREATE_TABLE_COLUMN, column=mock_column)
        re = f"^{mock_col_name} TEXT$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_225_sql_create_table_dt_column(self):
        mock_col_name = "mock_col_name"
        mock_column = (mock_col_name, dt)
        reply = self.db.sql(database.sql.SQL.CREATE_TABLE_COLUMN, column=mock_column)
        re = f"^{mock_col_name} DATETIME$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_226_sql_create_table_dt_column_default(self):
        mock_col_name = "mock_col_name"
        mock_column = (mock_col_name, dt, "dt")
        reply = self.db.sql(database.sql.SQL.CREATE_TABLE_COLUMN, column=mock_column)
        re = f"^{mock_col_name} DATETIME DEFAULT CURRENT_TIMESTAMP$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_227_sql_create_table_dt_column(self):
        mock_col_name = "mock_col_name"
        mock_column = (mock_col_name, date)
        reply = self.db.sql(database.sql.SQL.CREATE_TABLE_COLUMN, column=mock_column)
        re = f"^{mock_col_name} DATE$"
        self.assertRegex(reply.replace("\n", " "), re)

    def test_299_sql_any_other(self):
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
        self.db_cfg = {"file": "mock_sqlite_file.db"}
        self.con = database.sqlite.SQLiteConnection(self.mock_db, **self.db_cfg)
        return super().setUp()

    def test_001_connection(self):
        self.assertDictEqual(self.con._cfg, self.db_cfg)

    async def test_101_connect(self):
        mock_aioconnection = Mock(name="mock_aioconnection")
        mock_sqlite_connect = AsyncMock(
            name="mock_aioconnect", return_value=mock_aioconnection
        )
        with patch("db.sqlite.aiosqlite") as mock_sqlite:
            mock_sqlite.connect = mock_sqlite_connect
            reply = await self.con.connect()
        self.assertEqual(reply, self.con)
        mock_sqlite_connect.assert_awaited_once_with(database=self.db_cfg["file"])
        self.assertEqual(self.con._connection, mock_aioconnection)

        # test rowfactory
        mock_cursor = Mock()
        mock_result = {"mock_1": "val_1", "mock_2": 2, "mock_3": "val_3"}
        mock_cursor.description = [(m,) for m in mock_result.keys()]
        mock_row = tuple(mock_result.values())
        result = mock_aioconnection.row_factory(mock_cursor, mock_row)
        self.assertEqual(result, mock_result)

    async def _201_execute(self, params=DEFAULT, close=DEFAULT, commit=DEFAULT):
        sql = "ANY_SQL"
        mock_cur = AsyncMock()
        MockCursor = Mock(return_value=mock_cur)
        self.con._connection = AsyncMock()
        mock_aiocursor = "mock_cur"
        self.con._connection.cursor.return_value = mock_aiocursor
        self.con.commit = sentinel.COMMIT
        with (patch("db.sqlite.SQLiteCursor", MockCursor),):
            if params is DEFAULT and close is DEFAULT and commit is DEFAULT:
                reply = await self.con.execute(sql)
            elif params is not DEFAULT and close is DEFAULT and commit is DEFAULT:
                reply = await self.con.execute(sql, params=params)
            elif params is DEFAULT and close is not DEFAULT and commit is DEFAULT:
                reply = await self.con.execute(sql, close=close)
            elif params is not DEFAULT and close is not DEFAULT and commit is DEFAULT:
                reply = await self.con.execute(sql, params=params, close=close)
            elif params is DEFAULT and close is DEFAULT and commit is not DEFAULT:
                reply = await self.con.execute(sql, commit=commit)
            elif params is not DEFAULT and close is DEFAULT and commit is not DEFAULT:
                reply = await self.con.execute(sql, params=params, commit=commit)
            elif params is DEFAULT and close is not DEFAULT and commit is not DEFAULT:
                reply = await self.con.execute(sql, close=close, commit=commit)
            elif (
                params is not DEFAULT and close is not DEFAULT and commit is not DEFAULT
            ):
                reply = await self.con.execute(
                    sql, params=params, close=close, commit=commit
                )
            self.assertEqual(reply, mock_cur)
            MockCursor.assert_called_once_with(
                cur=self.con._connection.cursor.return_value,
                con=self.con,
            )
            self.con._connection.cursor.assert_called_once_with()
            mock_cur.execute.assert_awaited_once_with(sql, params=ANY, close=ANY)
            if commit is DEFAULT:
                self.assertEqual(self.con.commit, sentinel.COMMIT)
            else:
                self.assertEqual(self.con.commit, commit)
            return mock_cur.execute

    async def test_201_execute(self):
        exec = await self._201_execute()
        exec.assert_awaited_once_with(ANY, params=None, close=False)
        exec = await self._201_execute(params=sentinel.PARAMS)
        exec.assert_awaited_once_with(ANY, params=sentinel.PARAMS, close=False)
        exec = await self._201_execute(close=sentinel.CLOSE)
        exec.assert_awaited_once_with(ANY, params=None, close=sentinel.CLOSE)
        exec = await self._201_execute(params=sentinel.PARAMS, close=sentinel.CLOSE)
        exec.assert_awaited_once_with(ANY, params=sentinel.PARAMS, close=sentinel.CLOSE)
        exec = await self._201_execute(commit=sentinel.COMMIT)
        exec.assert_awaited_once_with(ANY, params=None, close=False)
        exec = await self._201_execute(params=sentinel.PARAMS, commit=sentinel.COMMIT)
        exec.assert_awaited_once_with(ANY, params=sentinel.PARAMS, close=False)
        exec = await self._201_execute(close=sentinel.CLOSE, commit=sentinel.COMMIT)
        exec.assert_awaited_once_with(ANY, params=None, close=sentinel.CLOSE)
        exec = await self._201_execute(
            params=sentinel.PARAMS, close=sentinel.CLOSE, commit=sentinel.COMMIT
        )
        exec.assert_awaited_once_with(ANY, params=sentinel.PARAMS, close=sentinel.CLOSE)


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

    async def _101_execute(self, params=DEFAULT, close=DEFAULT):
        query = "ANY_SQL"
        self.cur._last_query = "PREV_SQL"
        self.mock_aiocursor.reset_mock()
        self.mock_aiocursor.rowcount = 99
        self.cur._rowcount = 0
        if params is DEFAULT and close is DEFAULT:
            reply = await self.cur.execute(query)
        elif params is not DEFAULT and close is DEFAULT:
            reply = await self.cur.execute(query, params=params)
        elif params is DEFAULT and close is not DEFAULT:
            reply = await self.cur.execute(query, close=close)
        elif params is not DEFAULT and close is not DEFAULT:
            reply = await self.cur.execute(query, params=params, close=close)
        self.assertEqual(reply, self.cur)
        self.assertEqual(self.cur._last_query, query)
        self.assertEqual(self.cur._rowcount, 99)
        self.mock_aiocursor.execute.assert_awaited_once_with(query, ANY)
        return self.mock_aiocursor.execute

    async def test_101_execute(self):
        result = await self._101_execute()
        result.assert_awaited_once_with(ANY, None)
        result = await self._101_execute(sentinel.PARAMS)
        result.assert_awaited_once_with(ANY, sentinel.PARAMS)
        result = await self._101_execute(close=0)

        result.assert_awaited_once_with(ANY, None)

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
