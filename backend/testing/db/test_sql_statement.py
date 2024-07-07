import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from db.sqlexpression import Eq, SQLBetween, Row, Value

from db.sqlexecutable import (
    SQLExecutable,
    SQL,
    Select,
    CreateTable,
    Insert,
    InvalidSQLStatementException,
    SQLDataType,
    SQLStatement,
    SQLColumnDefinition,
    TableValuedQuery,
)


class MockColumnDefinition:

    def __init__(
        self, name: str, data_type: type, constraint: str = None, key_manager=None
    ):
        self.name = name
        self.constraint = constraint
        self.data_type = str(data_type)

    def get_sql(self):
        return f"{self.name} {self.data_type} {self.constraint}"


class MockSQLFactory:

    @classmethod
    def get_sql_class(self, sql_cls: type):
        if sql_cls.__name__ == "SQLColumnDefinition":
            return MockColumnDefinition
        return sql_cls


class MockDB(AsyncMock):
    execute = AsyncMock(return_value="Mock execute")
    close = AsyncMock(return_value="Mock close")

    sql_factory = MockSQLFactory


class MockApp:
    db = MockDB()


class AsyncTestSQLExecutable(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        mockParent = SQLExecutable()
        mockParent.execute = AsyncMock(return_value="Mock execute")
        mockParent.close = AsyncMock(return_value="Mock close")
        self.sql = SQLExecutable()
        self.sql._parent = mockParent

    async def test001_execute(self):
        sql_executable = self.sql

        # Test the execute method
        await sql_executable.execute(params="params", close=True, commit=False)
        sql_executable._parent.execute.assert_called_once()

    async def test002_close(self):
        sql_executable = self.sql

        # Test the close method
        await sql_executable.close()
        sql_executable._parent.close.assert_called_once()


@patch("db.sqlexecutable.App", MockApp)
class AsyncTestSQL(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        mock_db = MockDB()
        mock_db.execute = AsyncMock(return_value="Mock execute")
        mock_db.close = AsyncMock(return_value="Mock close")
        self.sql = SQL()

    async def test104_execute_default(self):
        """Test exception when no SQL statement is set"""
        # Test the execute method
        with self.assertRaises(InvalidSQLStatementException):
            await self.sql.execute()

    async def test105_execute(self):
        """Test direct execute method when an SQL statement is set"""

        # Test the execute method
        self.sql.create_table(
            "users",
            [("name", SQLDataType.TEXT, None), ("age", SQLDataType.INTEGER, None)],
        )
        await self.sql.execute()
        MockApp.db.execute.assert_awaited_once_with(
            self.sql.get_sql(), None, False, False
        )

    async def test106_execute_indirect(self):
        """Test indirect execute method when an SQL statement is set"""

        # Test the execute method
        self.sql.create_table(
            "users",
            [("name", SQLDataType.TEXT, None), ("age", SQLDataType.INTEGER, None)],
        )
        await self.sql._sql_statement.execute()
        MockApp.db.execute.assert_awaited_once_with(
            self.sql.get_sql(), None, False, False
        )

    async def test107_close(self):
        """Test direct execute method when an SQL statement is set"""

        # Test the close method
        self.sql.create_table(
            "users",
            [("name", SQLDataType.TEXT, None), ("age", SQLDataType.INTEGER, None)],
        )
        await self.sql.close()
        MockApp.db.close.assert_awaited_once()

    async def test108_close_indirect(self):
        """Test indirect execute method when an SQL statement is set"""

        # Test the close method
        self.sql.create_table(
            "users",
            [("name", SQLDataType.TEXT, None), ("age", SQLDataType.INTEGER, None)],
        )
        await self.sql._sql_statement.close()
        MockApp.db.close.assert_awaited_once()


@patch("db.sqlexecutable.App", MockApp)
class TestSQL(unittest.TestCase):

    def setUp(self) -> None:
        # mock_db = MockDB()
        # mock_db.execute = AsyncMock(return_value="Mock execute")
        # mock_db.close = AsyncMock(return_value="Mock close")

        self.sql = SQL()

    def checkPrimaryStatement(self, statement: SQLStatement, type):
        self.assertIsInstance(statement, type)
        self.assertEqual(self.sql._sql_statement, statement)
        self.assertEqual(statement._parent, self.sql)

    def test113_get_db(self):
        db = SQL._get_db()
        self.assertEqual(db, MockApp.db)

    def test101_CreateTable(self):
        sql = self.sql

        # Test the CreateTable method
        result = sql.create_table(
            "users", [("name", "TEXT", None), ("age", "INTEGER", None)]
        )
        self.checkPrimaryStatement(result, CreateTable)

    def test102_select(self):
        """Test the select method"""
        sql = self.sql

        # Test the select method
        result = sql.select(["name", "age"], distinct=True)
        self.checkPrimaryStatement(result, Select)
        self.assertTrue(sql._sql_statement._distinct)

    def test103_insert(self):
        """Test the insert method"""
        sql = self.sql

        # Test the insert method
        result = sql.insert("users", ["name", "age"])
        self.checkPrimaryStatement(result, Insert)

    def test109_sql(self):
        """Test the sql method"""

        with self.assertRaises(InvalidSQLStatementException):
            self.sql.sql()

    def test110_sql_select_without_from(self):
        """Test sql method when a select statement is set, but before a from statement is set"""

        # Test the sql method
        self.sql.select(["name", "age"], distinct=True)
        with self.assertRaises(InvalidSQLStatementException):
            self.sql.sql()

    def test111_sql_select(self):
        """Test sql method when a select statement is set"""

        # Test the sql method
        self.sql.select(["name", "age"], distinct=True).from_("users")
        result = self.sql.get_sql()
        self.assertEqual(result, "SELECT DISTINCT name, age FROM users")

    def test112_sql_selectStart(self):

        self.sql.select([], distinct=False).from_("users").where(Eq("id", "'test'"))
        result = self.sql.get_sql()
        self.assertEqual(result.strip(), "SELECT * FROM users WHERE  (id = 'test')")


class TestSQLStatement(unittest.TestCase):

    def test201_exception(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            SQLStatement().sql()


class TestSQLColumnDefinition(unittest.TestCase):

    def test301_name(self):
        """Test the name property"""
        with self.assertRaises(ValueError):
            SQLColumnDefinition("name", str)


class TestCreateTable(unittest.TestCase):

    def setUp(self) -> None:

        mockParent = Mock()
        mockParent.sqlFactory = MockSQLFactory
        self.mockParent = mockParent

    def test401_parent(self):
        """Test setting the parent"""
        test = CreateTable(parent=self.mockParent)
        self.assertEqual(test._parent, self.mockParent)

    def test402_table(self):
        """Test setting the table name"""

        test = CreateTable(table="test", parent=self.mockParent)
        self.assertEqual(test._table, "test")

    def test403_table(self):
        """Test creating a table with a single column"""

        for cur_type in SQLDataType:
            with self.subTest(type=cur_type):
                test = CreateTable(
                    columns=[("name", cur_type, "constraintFor" + str(cur_type))],
                    parent=self.mockParent,
                )
                self.assertEqual(len(test._columns), 1)
                for column in test._columns:
                    self.assertEqual(column.name, "name")
                    self.assertEqual(column.data_type, "SQLDataType." + cur_type.name)


class TestTableValuedQuery(unittest.TestCase):

    def test501_parent(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            TableValuedQuery(Mock()).sql()

    def test502_setParent(self):
        """Test setting the parent"""
        mockParent = Mock()
        test = TableValuedQuery(mockParent())
        self.assertEqual(test._parent, mockParent())


class TestSelect(unittest.TestCase):
    """Test the SQLExecutable.Select class"""

    def setUp(self) -> None:

        mock_parent = Mock()
        mock_from = Mock()
        mock_parent.sqlFactory = MockSQLFactory
        self.mock_parent = mock_parent
        self.mock_from = mock_from

    def test701_parent(self):
        """Test setting the parent"""
        test = Select(parent=self.mock_parent)
        self.assertEqual(test._parent, self.mock_parent)

    def test702_init_with_column_list(self):
        """Test initializing with a column list"""

        test = Select(["name", "age"], parent=self.mock_parent)
        self.assertEqual(test._column_list, ["name", "age"])

    def test703_init_without_column_list(self):
        """Test initializing without a column list"""

        test = Select(parent=self.mock_parent)
        self.assertEqual(test._column_list, [])

    def test704_init_with_distinct(self):
        """Test initializing with distinct"""

        test = Select(distinct=True)
        self.assertTrue(test.distinct)

    def test705_init_without_distinct(self):
        """Test initializing without distinct"""

        test = Select()
        self.assertFalse(test._distinct)

    def test706_test_from_required(self):
        """Test that a from statement is required before calling sql"""

        test = Select(parent=self.mock_parent)
        with self.assertRaises(InvalidSQLStatementException):
            test.sql()

    def test707_test_distinct_method(self):
        """Test the distinct method"""

        test = Select(parent=self.mock_parent)
        test.distinct()
        self.assertTrue(test._distinct)


if __name__ == "__main__":
    unittest.main()
