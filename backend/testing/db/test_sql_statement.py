import unittest
from unittest.mock import Mock, AsyncMock

from db.SQLExpression import (
    eq,
    SQL_between,
)

from db.SQLExecutable import (
    SQLExecutable,
    SQL,
    Select,
    Create_Table,
    Insert,
    InvalidSQLStatementException,
    SQLDataType,
    SQL_statement,
    SQL_column_definition,
    TableValuedQuery,
)


class MockColumnDefinition:
    def __init__(self, name: str, data_type: type, constraint: str = None):
        self.name = name
        self.constraint = constraint
        self.data_type = str(data_type)

    def sql(self):
        return f"{self.name} {self.data_type} {self.constraint}"


class MockSQLFactory:

    @classmethod
    def get_sql_class(self, sql_cls: type):
        if sql_cls.__name__ == "SQL_column_definition":
            return MockColumnDefinition
        return sql_cls


class MockDB:
    def execute(self, sql, params=None, close=False, commit=False):
        return "Mock execute"

    def close(self):
        return "Mock close"

    sqlFactory = MockSQLFactory


class AsyncTestSQLExecutable(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        mockParent = SQLExecutable()
        mockParent.execute = AsyncMock(return_value="Mock execute")
        mockParent.close = AsyncMock(return_value="Mock close")
        self.sql = SQLExecutable()
        self.sql.parent = mockParent

    async def test001_execute(self):
        SQLExecutable = self.sql

        # Test the execute method
        await SQLExecutable.execute(params="params", close=True, commit=False)
        SQLExecutable.parent.execute.assert_called_once()

    async def test002_close(self):
        SQLExecutable = self.sql

        # Test the close method
        await SQLExecutable.close()
        SQLExecutable.parent.close.assert_called_once()


class AsyncTestSQL(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        mockDB = MockDB()
        mockDB.execute = AsyncMock(return_value="Mock execute")
        mockDB.close = AsyncMock(return_value="Mock close")
        self.sql = SQL(mockDB)

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
        self.sql.db.execute.assert_called_once()

    async def test106_execute_indirect(self):
        """Test indirect execute method when an SQL statement is set"""

        # Test the execute method
        self.sql.create_table(
            "users",
            [("name", SQLDataType.TEXT, None), ("age", SQLDataType.INTEGER, None)],
        )
        await self.sql.sql_statement.execute()
        self.sql.db.execute.assert_called_once()

    async def test107_close(self):
        """Test direct execute method when an SQL statement is set"""

        # Test the close method
        self.sql.create_table(
            "users",
            [("name", SQLDataType.TEXT, None), ("age", SQLDataType.INTEGER, None)],
        )
        await self.sql.close()
        self.sql.db.close.assert_called_once()

    async def test108_close_indirect(self):
        """Test indirect execute method when an SQL statement is set"""

        # Test the close method
        self.sql.create_table(
            "users",
            [("name", SQLDataType.TEXT, None), ("age", SQLDataType.INTEGER, None)],
        )
        await self.sql.sql_statement.close()
        self.sql.db.close.assert_called_once()


class TestSQL(unittest.TestCase):

    def setUp(self) -> None:
        mockDB = MockDB()
        mockDB.execute = AsyncMock(return_value="Mock execute")
        mockDB.close = AsyncMock(return_value="Mock close")
        self.sql = SQL(mockDB)

    def checkPrimaryStatement(self, statement: SQL_statement, type):
        self.assertIsInstance(statement, type)
        self.assertEqual(self.sql.sql_statement, statement)
        self.assertEqual(statement.parent, self.sql)

    def test101_create_table(self):
        sql = self.sql

        # Test the create_table method
        result = sql.create_table(
            "users", [("name", "TEXT", None), ("age", "INTEGER", None)]
        )
        self.checkPrimaryStatement(result, Create_Table)

    def test102_select(self):
        """Test the select method"""
        sql = self.sql

        # Test the select method
        result = sql.select(["name", "age"], distinct=True)
        self.checkPrimaryStatement(result, Select)
        self.assertTrue(sql.sql_statement.distinct)

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
        self.sql.select(["name", "age"], distinct=True).From("users")
        result = self.sql.sql()
        self.assertEqual(result, "SELECT DISTINCT name, age FROM users")

    def test112_sql_selectStart(self):

        self.sql.select([], distinct=False).From("users").Where(eq("id", "'test'"))
        result = self.sql.sql()
        self.assertEqual(result.strip(), "SELECT * FROM users WHERE  (id = 'test')")


class TestSQL_statement(unittest.TestCase):

    def test201_exception(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            SQL_statement().sql()


class TestSQL_column_definition(unittest.TestCase):

    def test301_name(self):
        """Test the name property"""
        with self.assertRaises(ValueError):
            SQL_column_definition("name", str)


class TestCreate_Table(unittest.TestCase):

    def setUp(self) -> None:

        mockParent = Mock()
        mockParent.sqlFactory = MockSQLFactory
        self.mockParent = mockParent

    def test401_parent(self):
        """Test setting the parent"""
        test = Create_Table(parent=self.mockParent)
        self.assertEqual(test.parent, self.mockParent)

    def test402_table(self):
        """Test setting the table name"""

        test = Create_Table(table="test", parent=self.mockParent)
        self.assertEqual(test.table, "test")

    def test403_table(self):
        """Test creating a table with a single column"""

        for type in SQLDataType:
            with self.subTest(type=type):
                test = Create_Table(
                    columns=[("name", type, "constraintFor" + str(type))],
                    parent=self.mockParent,
                )
                self.assertEqual(len(test.columns), 1)
                for column in test.columns:
                    self.assertEqual(column.name, "name")
                    self.assertEqual(column.data_type, "SQLDataType." + type.name)


class TestTableValuedQuery(unittest.TestCase):

    def test501_parent(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            TableValuedQuery(Mock()).sql()

    def test502_setParent(self):
        """Test setting the parent"""
        mockParent = Mock()
        test = TableValuedQuery(mockParent())
        self.assertEqual(test.parent, mockParent())


class TestSQL_between(unittest.TestCase):

    def test601_between(self):
        result = SQL_between("age", 18, 25)
        self.assertEqual(result.sql(), " (age BETWEEN 18 AND 25) ")


if __name__ == "__main__":
    unittest.main()
