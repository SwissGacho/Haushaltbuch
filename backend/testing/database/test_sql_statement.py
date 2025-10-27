import unittest
from unittest import mock
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sql import SQL
from database.sql_key_manager import SQL_Dict
from database.sql_statement import (
    SQLStatement,
    SQLScript,
    SQLTemplate,
    CreateTable,
    CreateView,
    TableValuedQuery,
    Select,
    Insert,
    Update,
)
from database.sql_clause import SQLColumnDefinition
from database.sql_expression import Eq, And, ColumnName, Value
from business_objects.bo_descriptors import BOBaseBase, BOColumnFlag
from business_objects.business_attribute_base import BaseFlag
from core.exceptions import InvalidSQLStatementException


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

    def __init__(self, name: str, data_type: type, constraint=None, parent=None):
        self.name = name
        self.constraint = constraint
        self.data_type = data_type
        self.parent = parent

    def get_query(self):
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


def clean_sql(sql: str | SQL_Dict) -> str | SQL_Dict:
    if isinstance(sql, str):
        return " ".join(sql.strip().split())
    return {"query": " ".join(sql["query"].strip().split()), "params": sql["params"]}


class Test_400_SQLStatement(unittest.TestCase):

    def test_401_exception(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            SQLStatement().get_sql()


class Test_600_SQLScript(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher = patch("database.sql_executable.App", MockApp)
        self.patcher.start()
        self.mockParent = Mock(spec=SQL)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

    def tearDown(self):
        self.patcher.stop()

    def test_601_init_with_template(self):
        sql = SQLScriptWithMockTemplate(SQLTemplate.TABLEINFO, parent=self.mockParent)
        self.assertEqual(sql.get_sql(), {"query": MOCKTABLELIST, "params": {}})

    def test_602_init_with_not_implemented_template(self):
        with self.assertRaises(KeyError) as exc:
            SQLScriptWithMockTemplate(object(), parent=self.mockParent)

    def test_603_init_with_str(self):
        sql = SQLScript("SELECT * FROM table", parent=self.mockParent)
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), {"query": "SELECT * FROM table", "params": {}})

    def test_604_init_with_str_and_params(self):
        sql = SQLScript(
            "SELECT * FROM table WHERE id = :id", id=1, parent=self.mockParent
        )
        self.assertEqual(
            sql.get_sql(),
            {"query": "SELECT * FROM table WHERE id = :id", "params": {"id": 1}},
        )

    def test_605_init_calls_merge_params(self):
        with patch.object(SQLScript, "merge_params") as mock:
            SQLScript(
                "SELECT * FROM table WHERE id = :id", id=1, parent=self.mockParent
            )
            mock.assert_called_once_with(
                "SELECT * FROM table WHERE id = :id", {"id": 1}
            )


class Test_700_CreateTable(unittest.TestCase):

    def setUp(self) -> None:
        self.patcher = patch("database.sql_executable.App", MockApp)
        self.patcher.start()

        self.mockParent = Mock(spec=SQL)
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

    def test_701_create_table_as_select(self):
        """Test creating a table using AS SELECT"""
        create_table = CreateTable(table="test", parent=self.mockParent)
        create_table.as_select(column_list=["mick", "mack"]).from_("mocks")
        self.assertEqual(
            clean_sql(create_table.get_sql()),
            {
                "query": "CREATE TABLE test AS SELECT mick, mack FROM mocks",
                "params": {},
            },
        )

    def test_702_create_table_with_columns(self):
        """Test creating a table with multiple columns"""
        create_table = CreateTable(
            table="test", columns=self.col_desc[:2], parent=self.mockParent
        )
        self.assertEqual(
            clean_sql(create_table.get_sql()),
            {
                "query": "CREATE TABLE test (" + ", ".join(self.expected_sql[:2]) + ")",
                "params": {},
            },
        )

        (n, t, c, p) = self.col_desc[2]
        create_table.column(name=n, data_type=t, constraint=c, **p)
        self.assertEqual(len(create_table._columns), 3)
        self.assertEqual(
            clean_sql(create_table.get_sql()),
            {
                "query": "CREATE TABLE test (" + ", ".join(self.expected_sql[:3]) + ")",
                "params": {},
            },
        )

    def test_703_create_temp_table(self):
        """Test creating a table with multiple columns"""
        create_table = CreateTable(
            table="test",
            columns=self.col_desc[:2],
            temporary=True,
            parent=self.mockParent,
        )
        self.assertEqual(
            clean_sql(create_table.get_sql()),
            {
                "query": "CREATE TEMPORARY TABLE test ("
                + ", ".join(self.expected_sql[:2])
                + ")",
                "params": {},
            },
        )

    def test_704_create_table_without_columns(self):
        create_table = CreateTable("test", [], parent=self.mockParent)
        with self.assertRaises(InvalidSQLStatementException) as exc:
            create_table.get_sql()
        self.assertEqual(
            str(exc.exception),
            "CREATE TABLE statement must have at least one column or 'AS SELECT' clause.",
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
        expected_sql = (
            "CREATE TABLE test (name DB_TEXT Not Null, age DB_INTEGER Default)"
        )
        self.assertEqual(
            clean_sql(create_table.get_sql()), {"query": expected_sql, "params": {}}
        )


class MockTableValuedQuery(TableValuedQuery):
    def get_query(self) -> str:
        return "MockTableValuedQuery"


class Test_800_TableValuedQuery(unittest.TestCase):
    def setUp(self) -> None:
        self.mockParent = Mock(spec=SQL)
        # self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

    def test_801_get_query(self):
        """Test the exception method"""
        with self.assertRaises(NotImplementedError):
            TableValuedQuery(parent=self.mockParent).get_query()

    def test_802_get_sql(self):
        test = MockTableValuedQuery(parent=self.mockParent)
        self.assertEqual(
            test.get_sql(), {"query": "MockTableValuedQuery", "params": {}}
        )


class Test_900_Select(unittest.TestCase):
    """Test the SQLExecutable.Select class"""

    def setUp(self) -> None:
        self.patcher = patch("database.sql_executable.App", MockApp)
        self.patcher.start()
        self.mockParent = Mock(spec=SQL)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

        self.mock_tab_name = "MockTable"

    def tearDown(self):
        self.patcher.stop()

    def test_901_from(self):
        """Test setting the parent"""
        test = Select(parent=self.mockParent).from_(self.mock_tab_name)
        self.assertEqual(
            clean_sql(test.get_sql()),
            {"query": f"SELECT * FROM {self.mock_tab_name}", "params": {}},
        )

    def test_902_init_with_column_list(self):
        """Test initializing with a column list"""
        test = Select(["name", "age"], parent=self.mockParent).from_(self.mock_tab_name)
        self.assertEqual(
            clean_sql(test.get_sql()),
            {"query": f"SELECT name, age FROM {self.mock_tab_name}", "params": {}},
        )

    def test_903_init_without_column_list(self):
        """Test initializing without a column list"""
        test = Select(parent=self.mockParent).from_(self.mock_tab_name)
        self.assertEqual(
            clean_sql(test.get_sql()),
            {"query": f"SELECT * FROM {self.mock_tab_name}", "params": {}},
        )

    def test_904_init_with_distinct(self):
        """Test initializing with distinct"""
        test = Select(distinct=True, parent=self.mockParent).from_(self.mock_tab_name)
        self.assertEqual(
            clean_sql(test.get_sql()),
            {"query": f"SELECT DISTINCT * FROM {self.mock_tab_name}", "params": {}},
        )

    def test_905_init_without_distinct(self):
        """Test initializing without distinct"""
        test = Select(parent=self.mockParent).from_(self.mock_tab_name)
        self.assertFalse(test._distinct)
        self.assertEqual(
            clean_sql(test.get_sql()),
            {"query": f"SELECT * FROM {self.mock_tab_name}", "params": {}},
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
        test = Select(parent=self.mockParent).from_(self.mock_tab_name)
        self.assertFalse(test._distinct)
        test.distinct()
        self.assertTrue(test.distinct)
        self.assertEqual(
            clean_sql(test.get_sql()),
            {"query": f"SELECT DISTINCT * FROM {self.mock_tab_name}", "params": {}},
        )

    def test_908_complex_query_with_params(self):
        eq1 = Eq(ColumnName("id1"), Value("mock_val", "test1"))
        eq2 = Eq(ColumnName("id2"), Value("mock_val", "test2"))
        sql = SQL().select().from_("users").where(And([eq1, eq2]))
        self.assertEqual(
            clean_sql(sql.get_sql()),
            {
                "query": "SELECT * FROM users WHERE ((id1 = :mock_val) AND (id2 = :mock_val1))",
                "params": {"mock_val": "test1", "mock_val1": "test2"},
            },
        )
        sql.having(Eq(ColumnName("age"), Value("mock_val", 18)))
        self.assertEqual(
            clean_sql(sql.get_sql()),
            {
                "query": " ".join(
                    [
                        "SELECT * FROM users",
                        "WHERE ((id1 = :mock_val) AND (id2 = :mock_val1))",
                        "HAVING (age = :mock_val2)",
                    ]
                ),
                "params": {
                    "mock_val": "test1",
                    "mock_val1": "test2",
                    "mock_val2": 18,
                },
            },
        )


class Test_A00_Insert(unittest.TestCase):
    """Test the SQLExecutable.Insert class"""

    def setUp(self) -> None:
        self.patcher = patch("database.sql_executable.App", MockApp)
        self.patcher.start()
        self.mockParent = Mock(spec=SQL)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

    def tearDown(self):
        self.patcher.stop()

    def test_A01_insert_init_without_rows(self):
        """Test the insert SQLStatement without rows"""
        insert = Insert(table="users", rows=[], parent=self.mockParent)
        with self.assertRaises(InvalidSQLStatementException) as exc:
            insert.get_sql()
        self.assertEqual(
            str(exc.exception), "INSERT statement must have at least one row of values."
        )

    def test_A02_insert_init_single_row(self):
        """Test the insert SQLStatement with a single row"""
        insert = Insert(
            table="users",
            rows=[Value("name", "TheName"), Value("age", 42)],
            parent=self.mockParent,
        )
        self.assertEqual(
            clean_sql(insert.get_sql()),
            {
                "query": "INSERT INTO users (name, age) VALUES (:name, :age)",
                "params": {"name": "TheName", "age": 42},
            },
        )

    def test_A03_insert_init_multiple_rows(self):
        """Test the insert SQLStatement with multiple rows"""
        insert = Insert(
            table="users",
            rows=[
                [Value("name", "TheName"), Value("age", 42)],
                [Value("name", "AnotherName"), Value("age", 24)],
            ],
            parent=self.mockParent,
        )
        self.assertEqual(
            clean_sql(insert.get_sql()),
            {
                "query": "INSERT INTO users (name, age) VALUES (:name, :age), (:name1, :age2)",
                "params": {
                    "name": "TheName",
                    "age": 42,
                    "name1": "AnotherName",
                    "age2": 24,
                },
            },
        )

    def test_A04_insert_row(self):
        """Test the insert SQLStatement with added a single row"""
        insert = Insert(
            table="users",
            parent=self.mockParent,
        )
        insert.rows([Value("name", "TheName"), Value("age", 42)])
        self.assertEqual(
            clean_sql(insert.get_sql()),
            {
                "query": "INSERT INTO users (name, age) VALUES (:name, :age)",
                "params": {"name": "TheName", "age": 42},
            },
        )

    def test_A05_insert_rows(self):
        """Test the insert SQLStatement with added multiple rows"""
        insert = Insert(
            table="users",
            parent=self.mockParent,
        )
        insert.rows(
            [
                [Value("name", "TheName"), Value("age", 42)],
                [Value("name", "AnotherName"), Value("age", 24)],
            ]
        )
        insert.rows([Value("name", "TheThirdMan"), Value("age", 62)])
        self.assertEqual(
            clean_sql(insert.get_sql()),
            {
                "query": "INSERT INTO users (name, age) VALUES (:name, :age), (:name1, :age2), (:name3, :age4)",
                "params": {
                    "name": "TheName",
                    "age": 42,
                    "name1": "AnotherName",
                    "age2": 24,
                    "name3": "TheThirdMan",
                    "age4": 62,
                },
            },
        )

    def test_A06_insert_returning(self):
        """Test the insert SQLStatement with returning"""
        insert = Insert(
            table="users",
            rows=[Value("name", "TheName"), Value("age", 42)],
            parent=self.mockParent,
        )
        self.assertEqual(
            clean_sql(insert.returning("id").get_sql()),
            {
                "query": "INSERT INTO users (name, age) VALUES (:name, :age) RETURNING id",
                "params": {"name": "TheName", "age": 42},
            },
        )


class Test_B00_Update(unittest.TestCase):
    """Test the SQLExecutable.Update class"""

    def setUp(self) -> None:
        self.patcher = patch("database.sql_executable.App", MockApp)
        self.patcher.start()
        self.mockParent = Mock(spec=SQL)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

    def tearDown(self):
        self.patcher.stop()

    def test_B01_update_init_without_assignment(self):
        """Test the update SQLStatement with no assignment"""
        update = Update("users", parent=self.mockParent)
        with self.assertRaises(InvalidSQLStatementException) as exc:
            update.get_sql()
        self.assertEqual(
            str(exc.exception), "UPDATE statement must have at least one assignment."
        )

    def test_B02_update_with_assignment(self):
        """Test the update SQLStatement with a single assignment"""
        update = Update("users", parent=self.mockParent)
        update.assignment("name", Value("TheName"))
        self.assertEqual(
            clean_sql(update.get_sql()),
            {
                "query": "UPDATE users SET name = :param",
                "params": {"param": "TheName"},
            },
        )

    def test_B03_update_multiple_assignment(self):
        """Test the update SQLStatement with multiple assignment"""
        update = Update("users", parent=self.mockParent)
        update.assignment(["name"], Value("TheName"))
        update.assignment(["age"], Value(42))
        self.assertEqual(
            clean_sql(update.get_sql()),
            {
                "query": "UPDATE users SET name = :param, age = :param1",
                "params": {"param": "TheName", "param1": 42},
            },
        )

    def test_B04_update_multiple_assignment_and_where(self):
        """Test the update SQLStatement with multiple assignment and where clause"""
        update = Update("users", parent=self.mockParent)
        update.assignment(["name"], Value("TheName"))
        update.assignment(["age"], Value(42))
        update.where(Eq(ColumnName("id"), Value(1)))
        self.assertEqual(
            clean_sql(update.get_sql()),
            {
                "query": "UPDATE users SET name = :param, age = :param1 WHERE (id = :param2)",
                "params": {"param": "TheName", "param1": 42, "param2": 1},
            },
        )

    def test_B05_update_with_assignment_and_where_and_returning(self):
        """Test the update SQLStatement with assignment, where and returning clauses"""
        update = Update("users", parent=self.mockParent)
        update.assignment(["age"], Value(42))
        update.where(Eq(ColumnName("id"), Value(1)))
        update.returning("id")
        self.assertEqual(
            clean_sql(update.get_sql()),
            {
                "query": "UPDATE users SET age = :param WHERE (id = :param1) RETURNING id",
                "params": {"param": 42, "param1": 1},
            },
        )


class Test_C000_CreateView(unittest.TestCase):
    """Test the SQLExecutable.CreateView class"""

    def setUp(self) -> None:
        self.patcher = patch("database.sql_executable.App", MockApp)
        self.patcher.start()
        self.mockParent = Mock(spec=SQL)
        self.mockParent.sql_factory = MockSQLFactory
        self.mockParent._get_db = Mock(return_value=MockDB())

    def tearDown(self):
        self.patcher.stop()

    def test_C01_create_view(self):
        """Test the create view SQLStatement"""
        create_view = CreateView("user_view", parent=self.mockParent).from_("users")
        self.assertEqual(
            clean_sql(create_view.get_sql()),
            {
                "query": "CREATE VIEW user_view AS SELECT * FROM users",
                "params": {},
            },
        )

    def test_C02_create_temp_view(self):
        """Test the create view SQLStatement"""
        create_view = CreateView(
            "user_view", temporary=True, parent=self.mockParent
        ).from_("users")
        self.assertEqual(
            clean_sql(create_view.get_sql()),
            {
                "query": "CREATE TEMPORARY VIEW user_view AS SELECT * FROM users",
                "params": {},
            },
        )

    def test_C03_create_view_with_cols(self):
        """Test the create view SQLStatement"""
        create_view = CreateView(
            "user_view", ["u_num", "u_name"], ["id", "name"], parent=self.mockParent
        ).from_("users")
        self.assertEqual(
            clean_sql(create_view.get_sql()),
            {
                "query": "CREATE VIEW user_view ( u_num, u_name ) AS SELECT id, name FROM users",
                "params": {},
            },
        )

    def test_C04_complex_view_with_params(self):
        create_view = (
            CreateView("user_view", parent=self.mockParent)
            .from_("users")
            .where(
                And(
                    [
                        Eq(ColumnName("id1"), Value("mock_val", "test1")),
                        Eq(ColumnName("id2"), Value("mock_val", "test2")),
                    ]
                )
            )
        ).having(Eq(ColumnName("age"), Value("mock_val", 18)))
        self.assertEqual(
            clean_sql(create_view.get_sql()),
            {
                "query": " ".join(
                    [
                        "CREATE VIEW user_view AS",
                        "SELECT * FROM users",
                        "WHERE ((id1 = :mock_val) AND (id2 = :mock_val1))",
                        "HAVING (age = :mock_val2)",
                    ]
                ),
                "params": {
                    "mock_val": "test1",
                    "mock_val1": "test2",
                    "mock_val2": 18,
                },
            },
        )
