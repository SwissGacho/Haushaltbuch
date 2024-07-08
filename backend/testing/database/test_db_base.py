""" Test suite for DB connection base classes """

import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call

import database.db_base
import database.sql


class TestDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {"cfg1": "mick", "cfg2": "mack", "cfg3": "mock"}
        self.db = database.db_base.DB(**self.db_cfg)
        return super().setUp()

    def test_001_db(self):
        self.assertDictEqual(self.db._cfg, self.db_cfg)
        self.assertEqual(self.db._connections, set())

    def test_102_sql_callable_SELECT(self):
        params = {"columns": ["col1", "col2"], "table": "tab"}
        reply = self.db.sql(database.sql.SQL.SELECT, **params)
        print(f"{reply=}")
        self.assertEqual(reply, "SELECT col1,col2 FROM tab")

    def test_103_sql_no_value(self):
        with self.assertRaises(ValueError):
            self.db.sql(database.sql.SQL.TABLE_LIST)

    def _201_check_column(self, mock_sql):
        mock_attr = ("mock_attr", None)
        Mock_SQL = Mock()
        Mock_SQL.CREATE_TABLE_COLUMN = "MOCK_CREATE_TABLE_COLUMN"
        self.db.sql = Mock(return_value=mock_sql)
        with (patch("database.db_base.SQL", Mock_SQL),):
            result = self.db.check_column(mock_sql, mock_attr, "mock_tab")
        self.db.sql.assert_called_once_with(
            "MOCK_CREATE_TABLE_COLUMN", column=mock_attr
        )
        return result

    def test_201_check_column(self):
        self.assertTrue(self._201_check_column("MOCK SQL"))

    def test_202_check_column_no_tabcol(self):
        self.assertFalse(self._201_check_column(None))

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
            "db.db_base.Connection.connection", new_callable=PropertyMock
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
