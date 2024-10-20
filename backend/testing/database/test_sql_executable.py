import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sqlexecutable import SQL, SQLScript, CreateTable, SQLTemplate

from core.exceptions import InvalidSQLStatementException
from persistance.bo_descriptors import BOColumnFlag

MOCKCONSTRAINTMAP = {
    BOColumnFlag.BOC_NONE: "",
    BOColumnFlag.BOC_NOT_NULL: "NOT NULL",
    BOColumnFlag.BOC_DEFAULT: "DEFAULT",
}


class MockColumnDefinition:

    def __init__(
        self, name: str, data_type: type, constraint: str = None, key_manager=None
    ):
        self.name = name
        self.constraint = constraint
        self.data_type = self.type_map[data_type]

    type_map = {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
    }

    def get_sql(self):
        return (
            f"{self.name} {self.data_type} {MOCKCONSTRAINTMAP.get(self.constraint, '')}"
        )


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

MOCKTABLEINFO = "Tableinfo"
class SQLScriptWithMockTemplate(SQLScript):
    sql_templates = {
        SQLTemplate.TABLEINFO: MOCKTABLEINFO
    }


class MockApp:
    db = MockDB()


class TestSQLScript(unittest.TestCase):

    def test_init_with_template(self):
        sql = SQLScriptWithMockTemplate(SQLTemplate.TABLEINFO)
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), MOCKTABLEINFO)
        self.assertEqual(sql.get_params(), {})
    
    def test_init_with_not_implemented_template(self):
        with self.assertRaises(KeyError):
            sql = SQLScriptWithMockTemplate(None)

    def test_init_with_str(self):
        sql = SQLScript("SELECT * FROM table")
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table")
        self.assertEqual(sql.get_params(), {})

    def test_init_with_str_and_params(self):
        sql = SQLScript("SELECT * FROM table WHERE id = :id", id=1)
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table WHERE id = :id")
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_init_calls_register_and_replace_named_parameters(self):
        with patch.object(SQLScript, "_register_and_replace_named_parameters") as mock:
            SQLScript("SELECT * FROM table WHERE id = :id", id=1)
            mock.assert_called_once_with(
                "SELECT * FROM table WHERE id = :id", {"id": 1}
            )

    def test_init_calls_indirectly_create_param(self):
        with patch.object(SQLScript, "_create_param") as mock:
            SQLScript("SELECT * FROM table WHERE id = :id", id=1)
            mock.assert_called_once_with("id", 1)

    def test_create_param(self):
        sql = SQLScript("SELECT * FROM table")
        sql._create_param("id", 1)
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_multiple_params_with_same_key(self):
        sql = SQLScript("SELECT * FROM table")
        sql._create_param("id", 1)
        sql._create_param("id", 2)
        self.assertEqual(sql.get_params(), {"id": 1, "id1": 2})

    def test_create_param_calls_register_key(self):
        with patch.object(SQLScript, "register_key") as mock:
            sql = SQLScript("SELECT * FROM table")
            sql._create_param("id", 1)
            mock.assert_called_once_with("id")

    def test_register_and_replace_named_parameters(self):
        sql = SQLScript("Dummy")
        sql._register_and_replace_named_parameters(
            "SELECT * FROM table WHERE id = :id", {"id": 1}
        )
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_register_and_replace_multiple_named_parameters(self):
        sql = SQLScript("Dummy")
        sql._register_and_replace_named_parameters(":id = id", {"id": 1})
        sql._register_and_replace_named_parameters(":id = id", {"id": 2})
        self.assertEqual(sql.get_params(), {"id": 1, "id1": 2})

    def test_register_and_replace_named_parameters_calls_register_key(self):
        with patch.object(SQLScript, "_create_param") as mock:
            sql = SQLScript("Dummy")
            sql._register_and_replace_named_parameters(
                "SELECT * FROM table WHERE id = :id", {"id": 1}
            )
            mock.assert_called_once_with("id", 1)

    def test_register_and_replace_named_parameters_without_key_in_query(self):
        sql = SQLScript("Dummy")
        sql._register_and_replace_named_parameters("SELECT * FROM table", {"id": 1})
        self.assertEqual(sql.get_sql(), "Dummy")


class TestCreateTable(unittest.TestCase):

    def setUp(self) -> None:
        mock_parent = SQL()
        mock_parent.execute = AsyncMock(return_value="Mock execute")
        mock_parent.close = AsyncMock(return_value="Mock close")
        SQL._get_db = Mock(return_value=MockApp.db)
        self.mockParent = mock_parent

    def test_create_table_parent(self):
        create_table = CreateTable(
            "users",
            [
                ("name", str, "", {}),
                ("age", int, "", {}),
            ],
            self.mockParent,
        )
        self.assertEqual(create_table._parent, self.mockParent)

    def test_create_table_with_columns(self):
        create_table = CreateTable(
            "users",
            [("name", str, "", {}), ("age", int, "", {})],
            self.mockParent,
        )
        expected_sql = "CREATE TABLE IF NOT EXISTS users (name TEXT , age INTEGER )"
        self.assertEqual(create_table.get_sql(), expected_sql)
        self.assertEqual(create_table.get_params(), {})

    def test_create_table_without_columns(self):
        create_table = CreateTable("users", [], self.mockParent)
        with self.assertRaises(InvalidSQLStatementException):
            create_table.get_sql()

    def test_create_table_with_column_constraint(self):
        create_table = CreateTable(
            "users",
            [
                ("name", str, BOColumnFlag.BOC_NOT_NULL, {}),
                ("age", int, BOColumnFlag.BOC_DEFAULT, {}),
            ],
            self.mockParent,
        )
        expected_sql = (
            "CREATE TABLE IF NOT EXISTS users (name TEXT NOT NULL, age INTEGER DEFAULT)"
        )
        self.assertEqual(create_table.get_sql(), expected_sql)
        self.assertEqual(create_table.get_params(), {})

    def test_params_always_empty(self):
        create_table = CreateTable("users", [], self.mockParent)
        self.assertEqual(create_table.get_params(), {})
