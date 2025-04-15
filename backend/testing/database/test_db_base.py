"""Test suite for DB connection base classes"""

import logging
import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call, ANY


import database.db_base
from database.sqlexecutable import SQLExecutable
from database.sqlstatement import SQL
from database.sql_clause import SQLColumnDefinition
from persistance.bo_descriptors import BOColumnFlag


class MockFactory:
    def get_sql_class(self, cls):
        print(f"mock_factory {cls=}")
        return cls


class MockDB:
    sql_factory = MockFactory()


class MockSQL(SQLExecutable):
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls)

    def _get_db(self):
        return MockDB()


class TestDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {"cfg1": "mick", "cfg2": "mack", "cfg3": "mock"}
        self.db = database.db_base.DB(**self.db_cfg)
        return super().setUp()

    def test_001_db(self):
        self.assertDictEqual(self.db._cfg, self.db_cfg)
        self.assertEqual(self.db._connections, set())

    def _200_check_column(self, col_correct):

        mock_name = "mock_col"
        mock_type = "mock_typ"
        mock_constr = "mock_constr"
        mock_kwa = {"M": "m"}
        mock_col_def = f"{mock_name} {mock_type} {mock_constr} {mock_kwa}"
        if col_correct:
            mock_col = mock_col_def
        elif col_correct is None:
            mock_col = None
        else:
            mock_col = f"wrong def of col {mock_name}"
        mockColDef = Mock()
        mockColDef.get_query = Mock(return_value=mock_col_def)
        MockColDef = Mock(return_value=mockColDef)
        with (
            patch("database.db_base.SQL", MockSQL),
            patch("database.db_base.SQLColumnDefinition", MockColDef),
        ):
            result = self.db.check_column(
                tab="mock_tab",
                col=mock_col,
                name=mock_name,
                data_type=mock_type,
                constraint=mock_constr,
                **mock_kwa,
            )
        MockColDef.assert_called_once_with(
            mock_name, mock_type, mock_constr, parent=ANY, **mock_kwa
        )
        mockColDef.get_query.assert_called_once_with()
        return result

    def test_201_check_column(self):
        with self.assertNoLogs():
            self.assertTrue(self._200_check_column(True))

    def test_202_check_column_no_tabcol(self):
        with self.assertLogs(None, logging.ERROR) as err_msg:
            self.assertFalse(self._200_check_column(None))
            self.assertTrue(err_msg.output[0].find("is undefined") >= 0)

    def test_203_check_column_wrong_tabcol(self):
        with self.assertLogs(None, logging.ERROR) as err_msg:
            self.assertFalse(self._200_check_column(False))
            self.assertTrue(err_msg.output[0].find("is defined") >= 0)

    async def test_301_close(self):
        con1 = AsyncMock()
        con2 = AsyncMock()
        self.db._connections = {con1, con2}
        await self.db.close()
        con1.close.assert_awaited_once_with()
        con2.close.assert_awaited_once_with()


class TestDBConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_db = Mock()
        self.mock_db._connections = set()
        self.db_cfg = {"cfg1": "mick", "cfg2": "mack", "cfg3": "mock"}
        self.con = database.db_base.Connection(db_obj=self.mock_db, **self.db_cfg)
        return super().setUp()

    def test_001_Connection(self):
        self.assertDictEqual(self.con._cfg, self.db_cfg)
        self.assertEqual(self.con._db, self.mock_db)
        self.assertIsNone(self.con._connection)
        self.assertEqual(self.mock_db._connections, {self.con})

    async def test_201_close(self):
        mock_con = AsyncMock()
        self.con._connection = mock_con
        mock_close = AsyncMock()
        mock_con.close = mock_close
        await self.con.close()
        mock_close.assert_awaited_once_with()
        self.assertEqual(self.mock_db._connections, set())
        self.assertIsNone(self.con._connection)

    def test_301_connection_prop(self):
        with patch(
            "database.db_base.Connection.connection", new_callable=PropertyMock
        ) as mock_con:
            mock_con.return_value = "mock_con1"
            con = database.db_base.Connection(self.mock_db)
            self.assertEqual(con.connection, "mock_con1")

    async def test_401_commit(self):
        mock_con = AsyncMock()
        self.con._connection = mock_con
        mock_commit = AsyncMock()
        mock_con.commit = mock_commit
        await self.con.commit()
        mock_commit.assert_awaited_once_with()


class TestCursor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_con = Mock()
        self.mock_cur = AsyncMock()
        self.cur = database.db_base.Cursor(self.mock_cur, self.mock_con)
        return super().setUp()

    def test_001_Connection(self):
        self.assertEqual(self.cur._cursor, self.mock_cur)
        self.assertEqual(self.cur._connection, self.mock_con)

    async def test_101_rowcount_prop(self):
        self.cur._rowcount = 99
        reply = await self.cur.rowcount
        self.assertEqual(reply, 99)

    async def test_201_fetchall(self):
        mock_fetchall = AsyncMock(return_value="mock_fetched")
        self.cur._cursor.fetchall = mock_fetchall
        reply = await self.cur.fetchall()
        mock_fetchall.assert_awaited_once_with()
        self.assertEqual(reply, "mock_fetched")

    async def test_301_close(self):
        mock_close = AsyncMock()
        self.mock_cur.close = mock_close
        await self.cur.close()
        mock_close.assert_awaited_once_with()
        self.assertIsNone(self.cur._cursor)
        self.assertEqual(self.mock_cur.mock_calls, [call.__bool__(), call.close()])

    async def test_302_close_no_cur(self):
        self.mock_cur.__bool__.return_value = False
        await self.cur.close()
        self.assertEqual(self.mock_cur.mock_calls, [call.__bool__()])
