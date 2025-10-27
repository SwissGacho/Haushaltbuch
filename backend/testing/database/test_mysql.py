"""Test suite for MySQL attachement"""

from multiprocessing import pool
from re import A
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

    remove("asyncmy")


from core.exceptions import ConfigurationError
import database.sql_statement


class MockConnection:
    def __init__(self, name, version, ix=-1):
        self.name = name
        # mock_cursor = Mock(name="cursor")
        # mock_cursor.close = AsyncMock()
        # mock_cursor.fetchone = AsyncMock(return_value=version)
        # self.execute = AsyncMock(return_value=mock_cursor)
        self.ix = ix


class MockPool:
    def __init__(self):
        self.cons = []
        self.db_vers = {"version": "No Version"}

    async def acquire(self):
        i = 0
        while i <= len(self.cons):
            if i == len(self.cons):
                self.cons.append(None)
            if self.cons[i] is None:
                break
            i += 1
        self.cons[i] = MockConnection("mock_connection", self.db_vers, i)
        return self.cons[i]

    async def release(self, con):
        for i in range(len(self.cons)):
            if self.cons[i] == con:
                self.cons[i] = None
                break


class Test_001_MySQLDB__init__(unittest.TestCase):
    def setUp(self) -> None:
        self.db_cfg = {
            "host": "mockhost",
            "db": "mockdb",
            "user": "mockuser",
        }
        return super().setUp()

    def tearDown(self) -> None:
        database.dbms.mysql.ASYNCMY_IMPORT_ERROR = None
        return super().tearDown()

    def test_001_MySQLDB(self):
        database.dbms.mysql.ASYNCMY_IMPORT_ERROR = None
        with patch("database.dbms.db_base.DB.__init__") as mock_db_init:
            self.db = database.dbms.mysql.MySQLDB(**self.db_cfg)
            mock_db_init.assert_called_once_with(**self.db_cfg)

    def test_002_MySQLDB_no_lib(self):
        database.dbms.mysql.ASYNCMY_IMPORT_ERROR = ModuleNotFoundError("Mock Error")
        with (
            self.assertRaises(ModuleNotFoundError),
            patch("database.dbms.db_base.DB.__init__") as mock_db_init,
        ):
            database.dbms.mysql.MySQLDB(**self.db_cfg)
        mock_db_init.assert_not_called()


class Test_002_MySQLDB(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_cfg = {
            "host": "mockhost",
            "dbname": "mockdb",
            "dbuser": "mockuser",
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
        self.mock_pool = MockPool()
        self.create_pool = AsyncMock(return_value=self.mock_pool)
        self.patchers = {
            patch("database.dbms.mysql.asyncmy.create_pool", self.create_pool),
        }
        for patcher in self.patchers:
            patcher.start()
        return super().setUp()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    async def test_000_init_db(self):
        self.assertIsNone(self.db.connection_pool)
        self.create_pool.assert_not_awaited()

    async def test_001_create_pool(self):
        self.mock_pool.acquire = AsyncMock()
        self.mock_pool.release = AsyncMock()

        await self.db.create_pool()

        self.assertEqual(self.db.connection_pool, self.mock_pool)
        self.create_pool.assert_awaited_once_with(
            host=self.db_cfg["host"],
            db=self.db_cfg["dbname"],
            port=3306,
            user=self.db_cfg["dbuser"],
            password=self.db_cfg["password"],
            ssl=None,
            minsize=1,
            maxsize=50,
            echo=False,
            pool_recycle=3600,
        )

        self.db.connection_pool = "mock_pool"
        self.create_pool.reset_mock()
        await self.db.create_pool()

        self.assertEqual(self.db.connection_pool, "mock_pool")
        self.create_pool.assert_not_awaited()

        self.mock_pool.acquire.assert_not_awaited()
        self.mock_pool.release.assert_not_awaited()

    async def test_102_connect(self):
        mock_con_obj = AsyncMock()
        mock_con_obj.connect = AsyncMock(return_value=mock_con_obj)
        Mock_Connection = Mock(return_value=mock_con_obj)
        with (patch("database.dbms.mysql.MySQLConnection", Mock_Connection),):

            reply = await self.db.connect()

            self.assertEqual(reply, mock_con_obj)
            Mock_Connection.assert_called_once_with(db_obj=self.db, pool=self.mock_pool)
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
        with patch("database.dbms.mysql.SQL", self.MockSQL):
            reply = await self.db._get_table_info(mock_table)
            self.sql.script.assert_called_once_with(
                database.sql_statement.SQLTemplate.TABLEINFO, table=mock_table
            )
            self.sql.execute.assert_awaited_once_with()
            self.mockCur.fetchall.assert_awaited_once_with()
            self.assertDictEqual(reply, expected)


class Test_003_MySQLConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_db = Mock()
        self.mock_con = AsyncMock()
        self.db_cfg = {
            "host": "mock_host",
            "dbname": "mock_dbname",
            "dbuser": "mock_user",
            "password": "mock_pw",
        }
        self.mock_pool = MockPool()
        self.con = database.dbms.mysql.MySQLConnection(
            self.mock_db, self.mock_pool, **self.db_cfg
        )
        self.patchers = {
            # patch("database.dbms.mysql."),
        }
        for patcher in self.patchers:
            patcher.start()
        return super().setUp()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    def test_001_connection(self):
        self.assertDictEqual(self.con._cfg, self.db_cfg)
        self.assertEqual(self.con._db, self.mock_db)
        self.assertEqual(self.con._pool, self.mock_pool)

    async def _100_connect(self, mock_db, mock_dbcfg="MySQL", checked=False):
        if mock_dbcfg == "MySQL":
            db_error = "MariaDB" in mock_db
        else:
            db_error = "MariaDB" not in mock_db
        mock_conn = MockConnection("mock_connection", self.mock_pool.db_vers)
        self.mock_pool.acquire = AsyncMock(return_value=mock_conn)
        mock_cursor = Mock(name="cursor")
        mock_cursor.close = AsyncMock()
        mock_sql = AsyncMock(name="mock_sql")
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.script = Mock(return_value=mock_sql)
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_cursor.fetchone = AsyncMock(
            return_value={"version": f"mock_version: {mock_db}"}
        )
        database.dbms.mysql.MySQLConnection._version_checked = checked
        with (
            # patch("database.dbms.mysql.asyncmy") as mock_mysql,
            patch("database.dbms.mysql.SQL", return_value=mock_sql) as Mock_SQL,
            patch("database.dbms.mysql.get_config_item", return_value=mock_dbcfg),
        ):
            # =============================================
            if db_error and not checked:
                with self.assertRaises(ConfigurationError):
                    await self.con.connect()
                return
            else:
                reply = await self.con.connect()
            # ============================================
        self.assertEqual(reply, self.con)
        self.mock_pool.acquire.assert_awaited_once_with()
        self.assertEqual(self.con._connection.ix, -1)
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
        mock_cur = AsyncMock(name="mock_execute_cursor")
        MockCursor = Mock(name="MockCursor", return_value=mock_cur)
        self.con._connection = AsyncMock(name="mock_asyncmy_connection")
        self.con._connection.cursor = Mock(return_value=mock_cur)
        with (
            patch("database.dbms.mysql.MySQLCursor", MockCursor),
            patch("database.dbms.mysql.DictCursor", "mock_dict_cursor"),
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


class Test_004_MySQLCursor(unittest.IsolatedAsyncioTestCase):
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
