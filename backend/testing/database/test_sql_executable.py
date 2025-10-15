import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sql_executable import SQLExecutable, SQLManagedExecutable


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


def clean_sql(sql: str) -> str:
    return " ".join(sql.strip().split())


class AsyncTest_100_SQLExecutable(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.mockParent = Mock(spec=SQLExecutable)
        self.mockParent.execute = AsyncMock(return_value="Mock execute")
        self.mockParent.close = AsyncMock(return_value="Mock close")
        self.mockParent._get_db = Mock(return_value=MockDB())
        self.sql_executable = SQLExecutable(parent=self.mockParent)

    async def test_101_execute(self):
        # Test the execute method
        result = await self.sql_executable.execute()
        self.mockParent.execute.assert_awaited_once_with()
        self.assertEqual(result, "Mock execute")

    async def test_102_close(self):
        # Test the close method
        await self.sql_executable.close()
        self.mockParent.close.assert_called_once()


class Test_200_SQLManagedExecutable(unittest.TestCase):

    def setUp(self) -> None:
        self.mockParent = Mock(spec=SQLExecutable)
        self.mockParent._get_db = Mock(return_value=MockDB())
        self.sql_managed_executable = SQLManagedExecutable(parent=self.mockParent)

    def test_201_get_query(self):
        # Test the get_query method
        with self.assertRaises(NotImplementedError):
            self.sql_managed_executable.get_query()

    def test_202_get_sql(self):
        # Test the get_sql method
        with self.assertRaises(NotImplementedError):
            self.sql_managed_executable.get_sql()

    def test_203_get_sql_in_subclass(self):
        # Test the get_sql method in a subclass
        class SQLManagedExecutableSubclass(SQLManagedExecutable):
            def get_query(self):
                return "MOCKING SQL QUERY"

        subclass_instance = SQLManagedExecutableSubclass(parent=self.mockParent)
        subclass_instance.params = {"param": 1}
        self.assertEqual(
            subclass_instance.get_sql(),
            {"query": "MOCKING SQL QUERY", "params": {"param": 1}},
        )
