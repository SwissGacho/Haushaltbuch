from re import M
from sqlite3 import connect
import unittest
from unittest.mock import AsyncMock, Mock, patch, sentinel

from database.sql import SQL, SQLTransaction, SQLConnection
from database.sql_key_manager import SQL_Dict
from database.sql_expression import And, ColumnName, Eq, SQLString, Value
from database.sql_statement import CreateTable, CreateView, Insert, SQLStatement, Select
from database.sql_clause import SQLColumnDefinition
from core.base_objects import ConnectionBaseClass
from core.exceptions import InvalidSQLStatementException
from business_objects.bo_descriptors import BOBaseBase, BOColumnFlag
from business_objects.business_attribute_base import BaseFlag


class MockColumnDefinition(SQLColumnDefinition):
    type_map = {
        int: "DB_INTEGER",
        float: "DB_REAL",
        str: "DB_TEXT",
        dict: "DB_DICT_JSON",
        list: "DB_LIST_JSON",
        BOBaseBase: "DB_REFERENCE",
        BaseFlag: "DB_FLAG",
    }
    constraint_map = {
        BOColumnFlag.BOC_NONE: "",
        BOColumnFlag.BOC_NOT_NULL: "Not Null",
        BOColumnFlag.BOC_UNIQUE: "Unique",
        BOColumnFlag.BOC_PK: "Primary Key",
        BOColumnFlag.BOC_PK_INC: "Primary Key Autoincrement",
        BOColumnFlag.BOC_FK: "References {relation}",
        BOColumnFlag.BOC_DEFAULT: "Default",
        BOColumnFlag.BOC_DEFAULT_CURR: "Default Current Timestamp",
        # BOColumnFlag.BOC_INC: "not available ! @%?°",
        # BOColumnFlag.BOC_CURRENT_TS: "not available ! @%?°",
    }


class MockSQLFactory:
    @classmethod
    def get_sql_class(self, sql_cls: type):
        if sql_cls.__name__ == "SQLColumnDefinition":
            return MockColumnDefinition
        return sql_cls


class MockConnection(ConnectionBaseClass):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.connected = True
        self.commit = AsyncMock(name="DBcommit")
        self.rollback = AsyncMock(name="DBrollback")
        self.begin = AsyncMock(name="DBbegin")
        self.close = AsyncMock(name="DBclose")

    def reset_mock(self):
        self.commit.reset_mock()
        self.rollback.reset_mock()
        self.close.reset_mock()


mocked_connection = MockConnection()


class MockDB(AsyncMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connect = AsyncMock(name="DBconnect", return_value=mocked_connection)
        self.execute = AsyncMock(name="DBexecute")
        self.close = AsyncMock(name="DBclose")
        self.sql_factory = MockSQLFactory

    def reset_mock(self):
        super().reset_mock()
        mocked_connection.reset_mock()
        self.connect.reset_mock()
        self.execute.reset_mock()
        self.close.reset_mock()


class MockApp:
    db = MockDB()


class MockException(Exception):
    pass


def clean_sql(sql: str | SQL_Dict) -> str | SQL_Dict:
    if isinstance(sql, str):
        return " ".join(sql.strip().split())
    return {"query": " ".join(sql["query"].strip().split()), "params": sql["params"]}


class AsyncTest_200_SQL(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.patcher = patch("database.sql_executable.App", MockApp)
        self.patcher.start()
        MockApp.db.reset_mock()
        self.sql = SQL()

    def tearDown(self):
        self.patcher.stop()

    async def test_201_execute_default(self):
        """Test exception when no SQL statement is set"""
        with self.assertRaises(InvalidSQLStatementException) as exc:
            await self.sql.execute()
        self.assertEqual(str(exc.exception), "No SQL statement to execute.")
        MockApp.db.connect.assert_not_awaited()
        MockApp.db.execute.assert_not_awaited()

    async def test_202_execute(self):
        """Test execute method when an SQL statement is set"""
        self.sql.script("MOCK SQL")
        self.assertEqual(self.sql.connection, None, msg="Connection should not be set")
        await self.sql.execute()
        MockApp.db.connect.assert_awaited_once_with()
        MockApp.db.execute.assert_awaited_once_with(
            query="MOCK SQL", params={}, connection=mocked_connection
        )
        self.assertEqual(
            self.sql.connection, mocked_connection, msg="Connection should be set"
        )

    async def test_203_execute_with_connection(self):
        """Test execute method using an existing connection"""
        mock_conn = MockConnection()
        sql = SQL(connection=mock_conn)
        sql.script("MOCK SQL")
        self.assertEqual(sql.connection, mock_conn, msg="Connection should be set")
        await sql._sql_statement.execute()
        MockApp.db.connect.assert_not_awaited()
        MockApp.db.execute.assert_awaited_once_with(
            query="MOCK SQL", params={}, connection=mock_conn
        )
        self.assertEqual(sql.connection, mock_conn, msg="Connection should be same")

    async def test_204_execute_with_params(self):
        """Test indirect execute method when an SQL statement is set"""
        self.sql.script("MOCK SQL :param1 :param2", param1="test1", param2="test2")
        await self.sql._sql_statement.execute()
        MockApp.db.connect.assert_awaited_once_with()
        MockApp.db.execute.assert_awaited_once_with(
            query="MOCK SQL :param1 :param2",
            params={"param1": "test1", "param2": "test2"},
            connection=mocked_connection,
        )

    async def test_205_close(self):
        """Test close method"""
        await self.sql.connect()
        self.assertEqual(
            self.sql.connection, mocked_connection, msg="Connection should be set"
        )
        await self.sql.close()
        mocked_connection.close.assert_awaited_once_with()
        self.assertEqual(self.sql.connection, None, msg="Connection should not be set")

    async def test_206_close_with_connection(self):
        """Test close method when external connection is set"""
        mock_conn = MockConnection()
        sql = SQL(connection=mock_conn)
        self.assertEqual(sql.connection, mock_conn, msg="Connection should be set")
        await sql.close()
        mock_conn.close.assert_not_awaited()
        self.assertEqual(
            sql.connection, mock_conn, msg="Connection should not be reset"
        )

    async def test_207_commit(self):
        """Test commit method"""
        await self.sql.commit()
        mocked_connection.commit.assert_not_awaited()
        await self.sql.connect()
        self.assertEqual(
            self.sql.connection, mocked_connection, msg="Connection should be set"
        )
        await self.sql.commit()
        mocked_connection.commit.assert_awaited_once_with()

    async def test_208_rollback(self):
        """Test rollback method"""
        await self.sql.rollback()
        mocked_connection.rollback.assert_not_awaited()
        await self.sql.connect()
        self.assertEqual(
            self.sql.connection, mocked_connection, msg="Connection should be set"
        )
        await self.sql.rollback()
        mocked_connection.rollback.assert_awaited_once_with()

    def checkPrimaryStatement(self, statement: SQLStatement, type):
        self.assertIsInstance(statement, type)
        self.assertEqual(self.sql._sql_statement, statement)
        self.assertEqual(statement._parent, self.sql)

    def test_211_get_db(self):
        db = SQL._get_db()
        self.assertEqual(db, MockApp.db)

    def test_212_sql(self):
        """Test the get_sql method"""
        with self.assertRaises(InvalidSQLStatementException) as exc:
            self.sql.get_sql()
        self.assertEqual(str(exc.exception), "No SQL statement to execute.")

    def test_220_create_table(self):
        create_table = self.sql.create_table(
            "users", [("name", str, None, {}), ("age", int, None, {})]
        )
        self.checkPrimaryStatement(create_table, CreateTable)

    def test_221_create_view(self):
        create_view = self.sql.create_view("user_view")
        self.checkPrimaryStatement(create_view, CreateView)

    def test_230_select(self):
        """Test the select method"""
        select = self.sql.select(["name", "age"], distinct=True)
        self.checkPrimaryStatement(select, Select)
        self.assertTrue(self.sql._sql_statement._distinct)

    def test_231_sql_select_without_from(self):
        """Test get_sql method when a select statement is set, but before a from clause is set"""
        self.sql.select(["name", "age"], distinct=True)
        with self.assertRaises(InvalidSQLStatementException) as exc:
            self.sql.get_sql()
        self.assertEqual(
            str(exc.exception), "SELECT statement must have a FROM clause."
        )

    def test_232_sql_select_from(self):
        """Test get_sql method when a select statement is set"""
        self.sql.select(["name", "age"], distinct=True).from_("users")
        self.assertEqual(
            clean_sql(self.sql.get_sql()),
            {"query": "SELECT DISTINCT name, age FROM users", "params": {}},
        )

    def test_233_sql_select_where(self):
        """Test get_sql method when a select statement with where clause is set"""
        self.sql.select([], distinct=False).from_("users").where(
            Eq(ColumnName("id"), SQLString("test"))
        )
        self.assertEqual(
            clean_sql(self.sql.get_sql()),
            {"query": "SELECT * FROM users WHERE (id = 'test')", "params": {}},
        )

    def test_234_sql_select_with_params(self):
        """Test get_sql method when a select statement with where clause is set"""
        self.sql.select([], distinct=False).from_("users").where(
            And(
                [
                    Eq(ColumnName("id1"), Value("mock_val1", "test1")),
                    Eq(ColumnName("id2"), Value("mock_val2", "test2")),
                ]
            )
        )
        print(self.sql.get_sql())
        self.assertEqual(
            clean_sql(self.sql.get_sql()),
            {
                "query": "SELECT * FROM users WHERE ((id1 = :mock_val1) AND (id2 = :mock_val2))",
                "params": {"mock_val1": "test1", "mock_val2": "test2"},
            },
        )

    def test_240_insert(self):
        """Test the insert method"""
        insert = self.sql.insert("users", [("name", "TheName"), ("age", 42)])
        self.checkPrimaryStatement(insert, Insert)


class AsyncTest_300_SQLTransaction(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_sql = Mock()
        self.MockSQL = Mock(name="MockSQL", return_value=self.mock_sql)
        self.patchers = {
            patch("database.sql_executable.App", MockApp),
            patch("database.sql.SQL", self.MockSQL),
        }
        for patcher in self.patchers:
            patcher.start()
        MockApp.db.reset_mock()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    async def test_301_sql_in_ctx_no_conn(self):
        """Test creating SQL in context"""
        async with SQLTransaction() as trx:
            sql = trx.sql()
        MockApp.db.connect.assert_awaited_once_with()
        self.MockSQL.assert_called_once_with(
            connection=mocked_connection, auto_commit=False
        )
        self.assertEqual(sql, self.mock_sql)
        mocked_connection.commit.assert_awaited_once_with()
        mocked_connection.rollback.assert_not_awaited()
        mocked_connection.close.assert_awaited_once_with()

    async def test_302_sql_in_ctx_with_conn(self):
        """Test creating SQL in context with connection"""
        mock_external_conn = MockConnection()
        async with SQLTransaction(mock_external_conn) as trx:
            sql = trx.sql()
        MockApp.db.connect.assert_not_awaited()
        self.MockSQL.assert_called_once_with(
            connection=mock_external_conn, auto_commit=False
        )
        self.assertEqual(sql, self.mock_sql)
        mock_external_conn.commit.assert_awaited_once_with()
        mock_external_conn.rollback.assert_not_awaited()
        mock_external_conn.close.assert_not_awaited()
        mocked_connection.commit.assert_not_awaited()
        mocked_connection.rollback.assert_not_awaited()
        mocked_connection.close.assert_not_awaited()

    async def test_303_sql_in_ctx_no_conn_w_exc(self):
        """Test creating SQL in context with exception"""
        with self.assertRaises(MockException):
            async with SQLTransaction() as trx:
                sql = trx.sql()
                raise MockException("Test exception")
        MockApp.db.connect.assert_awaited_once_with()
        self.MockSQL.assert_called_once_with(
            connection=mocked_connection, auto_commit=False
        )
        self.assertEqual(sql, self.mock_sql)
        mocked_connection.commit.assert_not_awaited()
        mocked_connection.rollback.assert_awaited_once_with()
        mocked_connection.close.assert_awaited_once_with()

    async def test_304_sql_in_ctx_with_conn_w_exc(self):
        """Test creating SQL in context with connection and  exception"""
        mock_external_conn = Mock(name="ext. Conn.", spec=ConnectionBaseClass)
        mock_external_conn.commit = AsyncMock(return_value="Mock ext. commit")
        mock_external_conn.rollback = AsyncMock(return_value="Mock ext. rollback")
        mock_external_conn.begin = AsyncMock(return_value="Mock ext. begin")
        mock_external_conn.close = AsyncMock(return_value="Mock ext. close")
        with self.assertRaises(MockException):
            async with SQLTransaction(mock_external_conn) as trx:
                sql = trx.sql()
                raise MockException("Test exception")
        MockApp.db.connect.assert_not_awaited()
        self.MockSQL.assert_called_once_with(
            connection=mock_external_conn, auto_commit=False
        )
        self.assertEqual(sql, self.mock_sql)
        mock_external_conn.commit.assert_not_awaited()
        mock_external_conn.rollback.assert_awaited_once_with()
        mock_external_conn.close.assert_not_awaited()
        mocked_connection.commit.assert_not_awaited()
        mocked_connection.rollback.assert_not_awaited()
        mocked_connection.close.assert_not_awaited()

    def test_311_sql_no_ctx_no_conn(self):
        """Test creating SQL without with"""
        trx = SQLTransaction()
        sql = trx.sql()
        self.MockSQL.assert_called_once_with(connection=None, auto_commit=False)
        self.assertEqual(sql, self.mock_sql)

    def test_312_sql_no_ctx_with_conn(self):
        """Test creating SQL with connection"""
        trx = SQLTransaction(sentinel.connection)
        sql = trx.sql()
        self.MockSQL.assert_called_once_with(
            connection=sentinel.connection, auto_commit=False
        )
        self.assertEqual(sql, self.mock_sql)


class AsyncTest_400_SQLConnection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_sql = Mock()
        self.MockSQL = Mock(name="MockSQL", return_value=self.mock_sql)
        self.mock_trx = Mock()
        self.MockTrx = Mock(name="MockTrx", return_value=self.mock_trx)
        self.patchers = {
            patch("database.sql_executable.App", MockApp),
            patch("database.sql.SQL", self.MockSQL),
            patch("database.sql.SQLTransaction", self.MockTrx),
        }
        for patcher in self.patchers:
            patcher.start()
        MockApp.db.reset_mock()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    async def test_401_sql_trx_in_ctx(self):
        """Test creating SQL and Trx in context"""
        async with SQLConnection() as conn:
            MockApp.db.connect.assert_awaited_once_with()
            sql = conn.sql()
            self.assertEqual(sql, self.mock_sql)
            self.MockSQL.assert_called_once_with(connection=mocked_connection)
            self.MockTrx.assert_not_called()

            trx = conn.transaction()
            self.assertEqual(trx, self.mock_trx)
            MockApp.db.connect.assert_awaited_once_with()
            self.MockSQL.assert_called_once_with(connection=mocked_connection)
            self.MockTrx.assert_called_once_with(connection=mocked_connection)
            mocked_connection.commit.assert_not_awaited()
            mocked_connection.rollback.assert_not_awaited()
            mocked_connection.close.assert_not_awaited()
        mocked_connection.close.assert_awaited_once_with()

    async def test_403_sql_in_ctx_w_exc(self):
        """Test creating SQL in context with exception"""
        with self.assertRaises(MockException):
            async with SQLConnection() as conn:
                sql = conn.sql()
                raise MockException("Test exception")
        MockApp.db.connect.assert_awaited_once_with()
        self.MockSQL.assert_called_once_with(connection=mocked_connection)
        self.assertEqual(sql, self.mock_sql)
        mocked_connection.commit.assert_not_awaited()
        mocked_connection.rollback.assert_not_awaited()
        mocked_connection.close.assert_awaited_once_with()

    async def test_404_trx_in_ctx_w_exc(self):
        """Test creating Trx in context with exception"""
        with self.assertRaises(MockException):
            async with SQLConnection() as conn:
                trx = conn.transaction()
                raise MockException("Test exception")
        MockApp.db.connect.assert_awaited_once_with()
        self.MockTrx.assert_called_once_with(connection=mocked_connection)
        self.assertEqual(trx, self.mock_trx)
        mocked_connection.commit.assert_not_awaited()
        mocked_connection.rollback.assert_not_awaited()
        mocked_connection.close.assert_awaited_once_with()

    def test_405_sql_trx_no_ctx_no_conn(self):
        """Test creating SQL and Trx without with"""
        conn = SQLConnection()

        sql = conn.sql()
        self.MockSQL.assert_called_once_with(connection=None)
        self.assertEqual(sql, self.mock_sql)

        trx = conn.transaction()
        self.MockTrx.assert_called_once_with(connection=None)
        self.assertEqual(trx, self.mock_trx)

    async def test_406_sql_trx_no_ctx_connected(self):
        """Test creating SQL and Trx with connection"""
        conn = SQLConnection()
        await conn.connect()
        sql = conn.sql()
        self.MockSQL.assert_called_once_with(connection=mocked_connection)
        self.assertEqual(sql, self.mock_sql)
