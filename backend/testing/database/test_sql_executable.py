from calendar import c
import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

import database.sqlexecutable, database.sqlkeymanager
from database.sqlexpression import ColumnName, Eq, SQLBetween, And, SQLString, Value
from database.sqlexecutable import (
    SQLExecutable,
    SQL,
    Select,
    SQLScript,
    SQLTemplate,
    CreateTable,
    Insert,
    InvalidSQLStatementException,
    SQLStatement,
    SQLColumnDefinition,
    TableValuedQuery,
)

from persistance.bo_descriptors import BOBaseBase, BOColumnFlag
from persistance.business_attribute_base import BaseFlag


class MockColumnDefinition:
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

    def __init__(self, name: str, data_type: type, constraint=None, key_manager=None):
        self.name = name
        self.constraint = constraint
        self.data_type = data_type

    def get_sql(self):
        return " ".join(
            [
                self.name,
                self.__class__.type_map[self.data_type],
                (
                    self.constraint
                    if isinstance(self.constraint, str)
                    else self.__class__.constraint_map[self.constraint]
                ),
            ]
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


class MockApp:
    db = MockDB()


MOCKTABLELIST = "MockTableinfo"


class SQLScriptWithMockTemplate(SQLScript):
    sql_templates = {SQLTemplate.TABLEINFO: MOCKTABLELIST}


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
        await self.sql_executable.execute(close=True, commit=False)
        self.mockParent.execute.assert_awaited_once_with(close=True, commit=False)

    async def test_102_close(self):
        # Test the close method
        await self.sql_executable.close()
        self.mockParent.close.assert_called_once()


class AsyncTest_200_SQL(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.patcher = patch("database.sqlexecutable.App", MockApp)
        self.patcher.start()
        MockApp.db.execute.reset_mock()
        MockApp.db.close.reset_mock()
        self.sql = SQL()

    def tearDown(self):
        self.patcher.stop()

    async def test_201_execute_default(self):
        """Test exception when no SQL statement is set"""
        with self.assertRaises(InvalidSQLStatementException) as exc:
            await self.sql.execute()
        self.assertEqual(str(exc.exception), "No SQL statement to execute.")

    async def test_202_execute(self):
        """Test direct execute method when an SQL statement is set"""
        print(f"execute: {MockApp.db=}")
        self.sql.script("MOCK SQL")
        await self.sql.execute()
        MockApp.db.execute.assert_awaited_once_with("MOCK SQL", {}, False, False)

    async def test_203_execute_indirect(self):
        """Test indirect execute method when an SQL statement is set"""
        self.sql.script("MOCK SQL")
        await self.sql._sql_statement.execute()
        MockApp.db.execute.assert_awaited_once_with("MOCK SQL", {}, False, False)

    async def test_204_execute_with_params(self):
        """Test indirect execute method when an SQL statement is set"""
        self.sql.script("MOCK SQL :param1 :param2", param1="test1", param2="test2")
        await self.sql._sql_statement.execute()
        MockApp.db.execute.assert_awaited_once_with(
            "MOCK SQL :param1 :param2",
            {"param1": "test1", "param2": "test2"},
            False,
            False,
        )

    async def test_205_close(self):
        """Test direct execute method when an SQL statement is set"""
        self.sql.script("MOCK SQL")
        await self.sql.close()
        MockApp.db.close.assert_awaited_once_with()

    async def test_206_close_indirect(self):
        """Test indirect execute method when an SQL statement is set"""
        self.sql.script("MOCK SQL")
        await self.sql._sql_statement.close()
        MockApp.db.close.assert_awaited_once_with()


class Test_300_SQL(unittest.TestCase):

    def setUp(self) -> None:
        self.patcher = patch("database.sqlexecutable.App", MockApp)
        self.patcher.start()
        # mock_db = MockDB()
        # mock_db.execute = AsyncMock(return_value="Mock execute")
        # mock_db.close = AsyncMock(return_value="Mock close")

        self.sql = SQL()

    def tearDown(self):
        self.patcher.stop()

    def checkPrimaryStatement(self, statement: SQLStatement, type):
        self.assertIsInstance(statement, type)
        self.assertEqual(self.sql._sql_statement, statement)
        self.assertEqual(statement._parent, self.sql)

    def test_301_get_db(self):
        db = SQL._get_db()
        self.assertEqual(db, MockApp.db)

    def test_302_CreateTable(self):
        create_table = self.sql.create_table(
            "users", [("name", str, None, {}), ("age", int, None, {})]
        )
        self.checkPrimaryStatement(create_table, CreateTable)

    def test_303_select(self):
        """Test the select method"""
        select = self.sql.select(["name", "age"], distinct=True)
        self.checkPrimaryStatement(select, Select)
        self.assertTrue(self.sql._sql_statement._distinct)

    def test_304_insert(self):
        """Test the insert method"""
        insert = self.sql.insert("users", ["name", "age"])
        self.checkPrimaryStatement(insert, Insert)

    def test_310_sql(self):
        """Test the get_sql method"""
        with self.assertRaises(InvalidSQLStatementException) as exc:
            self.sql.get_sql()
        self.assertEqual(str(exc.exception), "No SQL statement to execute.")

    def test_311_sql_select_without_from(self):
        """Test get_sql method when a select statement is set, but before a from clause is set"""
        self.sql.select(["name", "age"], distinct=True)
        with self.assertRaises(InvalidSQLStatementException) as exc:
            self.sql.get_sql()
        self.assertEqual(
            str(exc.exception), "SELECT statement must have a FROM clause."
        )

    def test_312_sql_select(self):
        """Test get_sql method when a select statement is set"""
        self.sql.select(["name", "age"], distinct=True).from_("users")
        self.assertEqual(
            clean_sql(self.sql.get_sql()), "SELECT DISTINCT name, age FROM users"
        )

    def test_313_sql_selectStart(self):
        """Test get_sql method when a select statement with where clause is set"""
        self.sql.select([], distinct=False).from_("users").where(
            Eq(ColumnName("id"), SQLString("test"))
        )
        self.assertEqual(
            clean_sql(self.sql.get_sql()), "SELECT * FROM users WHERE (id = 'test')"
        )

    def test_314_sql_select_with_params(self):
        """Test get_sql method when a select statement with where clause is set"""
        self.sql.select([], distinct=False).from_("users").where(
            And(
                [
                    Eq(ColumnName("id1"), Value("mock_val1", "test1", key="v1")),
                    Eq(ColumnName("id2"), Value("mock_val2", "test2", key="v2")),
                ]
            )
        )
        self.assertEqual(
            clean_sql(self.sql.get_sql()),
            "SELECT * FROM users WHERE (id1 = :v1) AND (id2 = :v2)",
        )
        self.assertEqual(self.sql.get_params(), {"v1": "test1", "v2": "test2"})


class Test_400_SQLStatement(unittest.TestCase):

    def test_401_exception(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            SQLStatement().get_sql()


class Test_500_SQLColumnDefinition(unittest.TestCase):

    def test_501_unmapped_type(self):
        """Test the name property"""
        with self.assertRaises(ValueError):
            SQLColumnDefinition("name", str)


class Test_600_SQLScript(unittest.TestCase):
    def setUp(self) -> None:
        database.sqlkeymanager.SQLKeyManager._last_key = 0
        self.mockParent = Mock(spec=SQLExecutable)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

    def test_601_init_with_template(self):
        sql = SQLScriptWithMockTemplate(SQLTemplate.TABLEINFO, parent=self.mockParent)
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), MOCKTABLELIST)
        self.assertEqual(sql.get_params(), {})

    def test_602_init_with_not_implemented_template(self):
        with self.assertRaises(KeyError) as exc:
            SQLScriptWithMockTemplate(object(), parent=self.mockParent)

    def test_603_init_with_str(self):
        sql = SQLScript("SELECT * FROM table", parent=self.mockParent)
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table")
        self.assertEqual(sql.get_params(), {})

    def test_604_init_with_str_and_params(self):
        sql = SQLScript(
            "SELECT * FROM table WHERE id = :id", id=1, parent=self.mockParent
        )
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table WHERE id = :id")
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_605_init_calls_register_and_replace_named_parameters(self):
        with patch.object(SQLScript, "_register_and_replace_named_parameters") as mock:
            SQLScript(
                "SELECT * FROM table WHERE id = :id", id=1, parent=self.mockParent
            )
            mock.assert_called_once_with(
                "SELECT * FROM table WHERE id = :id", {"id": 1}
            )

    def test_606_init_calls_indirectly_create_param(self):
        with patch.object(SQLScript, "_create_param") as mock:
            SQLScript(
                "SELECT * FROM table WHERE id = :id", id=1, parent=self.mockParent
            )
            mock.assert_called_once_with("id", 1)

    def test_607_create_param(self):
        sql = SQLScript("SELECT * FROM table", parent=self.mockParent)
        sql._create_param("id", 1)
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_608_create_param_with_same_key(self):
        sql = SQLScript("SELECT * FROM table", parent=self.mockParent)
        sql._create_param("id", 1)
        sql._create_param("id", 2)
        self.assertEqual(sql.get_params(), {"id": 1, "id1": 2})

    def test_609_create_param_calls_register_key(self):
        with patch.object(SQLScript, "register_key") as mock:
            sql = SQLScript("SELECT * FROM table", parent=self.mockParent)
            sql._create_param("id", 1)
            mock.assert_called_once_with("id")

    def test_610_register_and_replace_named_parameters(self):
        sql = SQLScript("Dummy", parent=self.mockParent)
        sql._register_and_replace_named_parameters(
            "SELECT * FROM table WHERE id = :id", {"id": 1}
        )
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_611_register_and_replace_multiple_named_parameters(self):
        sql = SQLScript("Dummy", parent=self.mockParent)
        sql._register_and_replace_named_parameters(":id = id", {"id": 1})
        sql._register_and_replace_named_parameters(":id = id", {"id": 2})
        self.assertEqual(sql.get_params(), {"id": 1, "id1": 2})

    def test_612_register_and_replace_named_parameters_calls_register_key(self):
        with patch.object(SQLScript, "_create_param") as mock:
            sql = SQLScript("Dummy", parent=self.mockParent)
            sql._register_and_replace_named_parameters(
                "SELECT * FROM table WHERE id = :id", {"id": 1}
            )
            mock.assert_called_once_with("id", 1)

    def test_613_register_and_replace_named_parameters_without_key_in_query(self):
        sql = SQLScript("Dummy", parent=self.mockParent)
        sql._register_and_replace_named_parameters("SELECT * FROM table", {"id": 1})
        self.assertEqual(sql.get_sql(), "Dummy")


class Test_700_CreateTable(unittest.TestCase):

    def setUp(self) -> None:
        self.patcher = patch("database.sqlexecutable.App", MockApp)
        self.patcher.start()

        self.mockParent = Mock(spec=SQLExecutable)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())
        self.col_desc = [
            (f"{t.__name__}_column", t, f"FLAG_{f}", {})
            for t, f in [
                (int, 8),
                (float, 1),
                (str, 64),
                (dict, 32),
                (list, 0),
                (BOBaseBase, 16),
            ]
        ]
        self.expected_sql = [
            "int_column DB_INTEGER FLAG_8",
            "float_column DB_REAL FLAG_1",
            "str_column DB_TEXT FLAG_64",
            "dict_column DB_DICT_JSON FLAG_32",
            "list_column DB_LIST_JSON FLAG_0",
            "BOBaseBase_column DB_REFERENCE FLAG_16",
        ]

    def tearDown(self):
        self.patcher.stop()

    def test_701_parent(self):
        """Test setting the parent"""
        create_table = CreateTable(parent=self.mockParent)
        self.assertEqual(create_table._parent, self.mockParent)

    def test_702_table_name(self):
        """Test setting the table name"""
        create_table = CreateTable(table="test", parent=self.mockParent)
        self.assertEqual(create_table._table, "test")

    def test_703_create_table_with_columns(self):
        """Test creating a table with multiple columns"""
        create_table = CreateTable(
            table="test", columns=self.col_desc[:2], parent=self.mockParent
        )
        self.assertEqual(len(create_table._columns), 2)
        self.assertIsInstance(create_table, CreateTable)
        self.assertEqual(
            clean_sql(create_table.get_sql()),
            "CREATE TABLE IF NOT EXISTS test ("
            + ", ".join(self.expected_sql[:2])
            + ")",
        )

        (n, t, c, p) = self.col_desc[2]
        create_table.column(name=n, data_type=t, constraint=c, **p)
        self.assertEqual(len(create_table._columns), 3)
        self.assertEqual(
            clean_sql(create_table.get_sql()),
            "CREATE TABLE IF NOT EXISTS test ("
            + ", ".join(self.expected_sql[:3])
            + ")",
        )

    def test_704_create_table_without_columns(self):
        create_table = CreateTable("test", [], parent=self.mockParent)
        with self.assertRaises(InvalidSQLStatementException) as exc:
            create_table.get_sql()
        self.assertEqual(
            str(exc.exception), "CREATE TABLE statement must have at least one column."
        )

    def test_705_create_table_with_column_constraint(self):
        create_table = CreateTable(
            "test",
            [
                ("name", str, BOColumnFlag.BOC_NOT_NULL, {}),
                ("age", int, BOColumnFlag.BOC_DEFAULT, {}),
            ],
            parent=self.mockParent,
        )
        expected_sql = "CREATE TABLE IF NOT EXISTS test (name DB_TEXT Not Null, age DB_INTEGER Default)"
        self.assertEqual(clean_sql(create_table.get_sql()), expected_sql)
        self.assertEqual(create_table.get_params(), {})

    def test_706_params_always_empty(self):
        create_table = CreateTable("test", [], parent=self.mockParent)
        self.assertEqual(create_table.get_params(), {})


class Test_800_TableValuedQuery(unittest.TestCase):
    def setUp(self) -> None:
        self.mockParent = Mock(spec=SQLExecutable)
        # self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

    def test_801_get_sql(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            TableValuedQuery(parent=self.mockParent).get_sql()

    def test_802_set_parent(self):
        """Test setting the parent"""
        test = TableValuedQuery(parent=self.mockParent)
        self.assertEqual(test._parent, self.mockParent)


class Test_900_Select(unittest.TestCase):
    """Test the SQLExecutable.Select class"""

    def setUp(self) -> None:
        self.mockParent = Mock(spec=SQLExecutable)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

        self.mock_tab_name = "MockTable"

    def test_901_from(self):
        """Test setting the parent"""
        test = Select(parent=self.mockParent)
        self.assertEqual(test._parent, self.mockParent)
        self.assertEqual(
            clean_sql(test.from_(self.mock_tab_name).get_sql()),
            f"SELECT * FROM {self.mock_tab_name}",
        )

    def test_902_init_with_column_list(self):
        """Test initializing with a column list"""
        test = Select(["name", "age"], parent=self.mockParent)
        self.assertEqual(test._column_list, ["name", "age"])
        self.assertEqual(
            clean_sql(test.from_(self.mock_tab_name).get_sql()),
            f"SELECT name, age FROM {self.mock_tab_name}",
        )

    def test_903_init_without_column_list(self):
        """Test initializing without a column list"""
        test = Select(parent=self.mockParent)
        self.assertEqual(test._column_list, [])
        self.assertEqual(
            clean_sql(test.from_(self.mock_tab_name).get_sql()),
            f"SELECT * FROM {self.mock_tab_name}",
        )

    def test_904_init_with_distinct(self):
        """Test initializing with distinct"""
        test = Select(distinct=True, parent=self.mockParent)
        self.assertTrue(test.distinct)
        self.assertEqual(
            clean_sql(test.from_(self.mock_tab_name).get_sql()),
            f"SELECT DISTINCT * FROM {self.mock_tab_name}",
        )

    def test_905_init_without_distinct(self):
        """Test initializing without distinct"""
        test = Select(parent=self.mockParent)
        self.assertFalse(test._distinct)
        self.assertEqual(
            clean_sql(test.from_(self.mock_tab_name).get_sql()),
            f"SELECT * FROM {self.mock_tab_name}",
        )

    def test_906_test_from_required(self):
        """Test that a from statement is required before calling sql"""
        test = Select(parent=self.mockParent)
        with self.assertRaises(InvalidSQLStatementException) as exc:
            test.get_sql()
        self.assertEqual(
            str(exc.exception), "SELECT statement must have a FROM clause."
        )

    def test_907_test_distinct_method(self):
        """Test the distinct method"""
        test = Select(parent=self.mockParent)
        self.assertFalse(test._distinct)
        test.distinct()
        self.assertTrue(test.distinct)
        self.assertEqual(
            clean_sql(test.from_(self.mock_tab_name).get_sql()),
            f"SELECT DISTINCT * FROM {self.mock_tab_name}",
        )


class Test_A00_SQL_between(unittest.TestCase):

    def test_A01_between(self):
        result = SQLBetween("age", 18, 25)
        self.assertEqual(result.get_sql(), " (age  BETWEEN  18  AND  25) ")


if __name__ == "__main__":
    unittest.main()
