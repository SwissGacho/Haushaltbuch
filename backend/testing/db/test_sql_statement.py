import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call

from db.sql_statement import SQL_Executable, SQL, Select, Create_Table, Insert, InvalidSQLStatementException, SQL_data_type, SQL_statement, SQL_column_definition

    
class MockDB:
    def execute(self, sql, params=None, close=False, commit=False):
        return "Mock execute"
    def close(self):
        return "Mock close"


class TestSQLExecutable(unittest.TestCase):

    def setUp(self) -> None:
        mockParent = SQL_Executable()
        mockParent.execute = Mock(return_value="Mock execute")
        mockParent.close = Mock(return_value="Mock close")
        self.sql = SQL_Executable()
        self.sql.parent = mockParent()

    def test001_execute(self):
        sql_executable = self.sql

        # Test the execute method
        sql_executable.execute(params="params", close=True, commit=False)
        sql_executable.parent.execute.assert_called_once()

    def test002_close(self):
        sql_executable = self.sql

        # Test the close method
        sql_executable.close()
        sql_executable.parent.close.assert_called_once()


class TestSQL(unittest.TestCase):

    def setUp(self) -> None:
        mockDB = MockDB()
        mockDB.execute = Mock(return_value="Mock execute")
        mockDB.close = Mock(return_value="Mock close")
        self.sql = SQL(mockDB)

    def checkPrimaryStatement(self, statement:SQL_statement, type):
        self.assertIsInstance(statement, type)
        self.assertEqual(self.sql.sql_statement, statement)
        self.assertEqual(statement.parent, self.sql)

    def test101_create_table(self):
        sql = self.sql

        # Test the create_table method
        result = sql.create_table("users", [("name", "TEXT"), ("age", "INTEGER")])
        self.checkPrimaryStatement(result, Create_Table)

    def test102_select(self):
        """ Test the select method"""
        sql = self.sql

        # Test the select method
        result = sql.select(["name", "age"], distinct=True)
        self.checkPrimaryStatement(result, Select)
        self.assertTrue(sql.sql_statement.distinct)

    def test103_insert(self):
        """ Test the insert method """
        sql = self.sql

        # Test the insert method
        result = sql.insert("users", ["name", "age"])
        self.checkPrimaryStatement(result, Insert)

    def test104_execute_default(self):
        """ Test exception when no SQL statement is set """
        # Test the execute method
        with self.assertRaises(InvalidSQLStatementException):
            self.sql.execute()

    def test105_execute(self):
        """ Test direct execute method when an SQL statement is set """

        # Test the execute method
        self.sql.create_table("users", [("name", SQL_data_type.TEXT), ("age", SQL_data_type.INTEGER)])
        self.sql.execute()
        self.sql.db.execute.assert_called_once()

    def test106_execute_indirect(self):
        """ Test indirect execute method when an SQL statement is set """

        # Test the execute method
        self.sql.create_table("users", [("name", SQL_data_type.TEXT), ("age", SQL_data_type.INTEGER)])
        self.sql.sql_statement.execute()
        self.sql.db.execute.assert_called_once()

    def test107_close(self):
        """ Test direct execute method when an SQL statement is set """

        # Test the close method
        self.sql.create_table("users", [("name", SQL_data_type.TEXT), ("age", SQL_data_type.INTEGER)])
        self.sql.close()
        self.sql.db.close.assert_called_once()

    def test108_close_indirect(self):
        """ Test indirect execute method when an SQL statement is set """

        # Test the close method
        self.sql.create_table("users", [("name", SQL_data_type.TEXT), ("age", SQL_data_type.INTEGER)])
        self.sql.sql_statement.close()
        self.sql.db.close.assert_called_once()

    def test109_sql(self):
        """ Test the sql method """

        with self.assertRaises(InvalidSQLStatementException):
            self.sql.sql()

    def test110_sql_select_without_from(self):
        """ Test sql method when a select statement is set, but before a from statement is set"""
            
        # Test the sql method
        self.sql.select(["name", "age"], distinct=True)
        with self.assertRaises(InvalidSQLStatementException):
            self.sql.sql()

    def test111_sql_select(self):
        """ Test sql method when a select statement is set """
            
        # Test the sql method
        self.sql.select(["name", "age"], distinct=True).From("users")
        result = self.sql.sql()
        self.assertEqual(result, "SELECT DISTINCT name, age FROM users")


class TestSQL_statement(unittest.TestCase):

    def test201_exception(self):
        """ Test the exception method """
        with self.assertRaises(NotImplementedError):
            SQL_statement().sql()


class TestSQL_column_definition(unittest.TestCase):

    def test301_name(self):
        """ Test the name property """
        result = SQL_column_definition("name", SQL_data_type.TEXT)
        self.assertEqual(result.name, "name")

    def test302_data_type(self):
        """ Test the data_type property """
        for type in SQL_data_type:
            with self.subTest(type=type):
                result = SQL_column_definition("name", type)
                self.assertEqual(result.data_type, type)

if __name__ == "__main__":
    unittest.main()