""" Test suite for DB connection base classes """

import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call

import db.db_base


class TestDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db = db.db_base.DB()
        return super().setUp()

    def test_001_no_string_in_enum(self):
        for sql in db.db_base.SQL:
            self.assertNotIsInstance(sql.value, str, msg=f"SQL.{sql.name}")

    def test_002_sql_callable_SELECT(self):
        params = {"columns": ["col1", "col2"], "table": "tab"}
        reply = self.db.sql(db.db_base.SQL.SELECT, **params)
        print(f"{reply=}")
        self.assertEqual(reply, "SELECT col1,col2 FROM tab")

    def test_003_sql_no_value(self):
        with self.assertRaises(ValueError):
            self.db.sql(db.db_base.SQL.TABLE_LIST)

    async def test_101_check(self):
        with self.assertRaises(TypeError):
            await self.db.check()

    async def test_201_close(self):
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
        self.mock_con = AsyncMock()
        self.con = db.db_base.Connection(self.mock_db, self.mock_con)
        return super().setUp()

    def test_001_Connection(self):
        self.assertEqual(self.con._db, self.mock_db)
        self.assertEqual(self.con._connection, self.mock_con)
        self.assertEqual(self.mock_db._connections, {self.con})

    async def test_201_close(self):
        self.con._connection.close = AsyncMock()
        await self.con.close()
        self.mock_con.close.assert_awaited_once_with()
        self.assertEqual(self.mock_db._connections, set())
        self.assertIsNone(self.con._connection)

    def test_301_connection_prop(self):
        with patch(
            "db.db_base.Connection.connection", new_callable=PropertyMock
        ) as mock_con:
            mock_con.return_value = "mock_con1"
            con = db.db_base.Connection(self.mock_db)
            self.assertEqual(con.connection, "mock_con1")
            con.connection = "mock_con2"
            self.assertEqual(mock_con.mock_calls, [call(), call("mock_con2")])

    async def test_401_commit(self):
        mock_commit = AsyncMock()
        self.con._connection.commit = mock_commit
        await self.con.commit()
        mock_commit.assert_awaited_once_with()


class TestCursor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_con = Mock()
        self.mock_cur = AsyncMock()
        self.cur = db.db_base.Cursor(self.mock_cur, self.mock_con)
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
