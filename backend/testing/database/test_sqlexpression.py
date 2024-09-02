import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sqlexpression import (
    SQLExpression,
    From,
    JoinOperator,
    SQLMultiExpression,
    And,
    Or,
    Eq,
    SQLColumnDefinition,
)

from database.sqlexpression import Value, Row


class MockKeyManager:
    register_key = Mock(side_effect=lambda x: x)


class MockKeyManagerWithModifications(MockKeyManager):
    register_key = Mock(side_effect=lambda x: x + "1")


def normalize_sql(sql):
    return re.sub("  +", " ").strip()


@patch("database.sqlkeymanager.SQLKeyManager", MockKeyManager)
class TestSQLExpression(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def testSQLExpressionInstantiation(self):
        sql_expression_instance = SQLExpression()
        self.assertIsNotNone(
            sql_expression_instance, "SQLExpression instance should not be None"
        )

    def testInitDefaults(self):
        sql = SQLExpression()
        self.assertEqual(sql._key_manager, None)
        self.assertEqual(sql._expression, "Null")
        self.assertEqual(sql._params, {})
        self.assertEqual(sql.get_sql(), "Null")
        self.assertEqual(sql.get_params(), {})

    def testcreate_param(self):
        sql = SQLExpression(key_manager=self.SQLKeyManager)
        key = sql.create_param("key", "value")
        self.SQLKeyManager.register_key.assert_called_once_with("key")
        self.assertEqual(key, "key")
        self.assertEqual(sql._params["key"], "value")
        self.assertEqual(sql.get_params(), {"key": "value"})

    def testsimpleexpression(self):
        sql = SQLExpression(expression="SELECT * FROM table")
        self.assertEqual(sql.get_sql(), "SELECT * FROM table")


class TestFrom(unittest.TestCase):

    def test_init(self):
        sql = From("table")
        self.assertEqual(sql._table, "table")
        self.assertEqual(sql.get_sql(), " FROM table ")
        self.assertEqual(sql.get_params(), {})
        self.assertEqual(sql._joins, [])

    def test_join(self):
        sql = From("table")
        sql.join("table2", None)
        self.assertEqual(sql.get_sql(), " FROM table FULL OUTER JOIN table2 ")
        self.assertEqual(sql.get_params(), {})
        self.assertEqual(sql._joins, [(JoinOperator.FULL, "table2", None)])

    def test_join_with_condition(self):
        sql = From("table")
        condition = Eq("table.id", "table2.id")
        sql.join("table2", condition)
        self.assertEqual(
            sql.get_sql().replace("  ", " ").strip(),
            "FROM table FULL OUTER JOIN table2 ON (table.id = table2.id)",
        )
        self.assertEqual(sql.get_params(), {})
        self.assertEqual(sql._joins, [(JoinOperator.FULL, "table2", condition)])

    def test_join_with_left_join(self):
        sql = From("table")
        condition = Eq("table.id", "table2.id")
        sql.join("table2", condition, JoinOperator.LEFT)
        self.assertEqual(
            sql.get_sql().replace("  ", " ").strip(),
            "FROM table LEFT JOIN table2 ON (table.id = table2.id)",
        )
        self.assertEqual(sql.get_params(), {})
        self.assertEqual(sql._joins, [(JoinOperator.LEFT, "table2", condition)])

    def test_join_with_multiple_joins(self):
        sql = From("table")
        condition = Eq("table.id", "table2.id")
        sql.join("table2", condition, JoinOperator.LEFT)
        sql.join("table3", None)
        self.assertEqual(
            sql.get_sql().replace("  ", " ").strip(),
            "FROM table LEFT JOIN table2 ON (table.id = table2.id) FULL OUTER JOIN table3",
        )
        self.assertEqual(sql.get_params(), {})
        self.assertEqual(
            sql._joins,
            [
                (JoinOperator.LEFT, "table2", condition),
                (JoinOperator.FULL, "table3", None),
            ],
        )


class TestSQLMultiExpression(unittest.TestCase):

    def setUp(self):
        self.expressions = [SQLExpression(), SQLExpression(), SQLExpression()]

    def test_init(self):
        sql = SQLMultiExpression(self.expressions)
        self.assertEqual(sql._arguments, self.expressions)

    def test_not_implemented(self):
        sql = SQLMultiExpression(self.expressions)
        self.assertRaises(NotImplementedError, sql.get_sql)
        self.assertRaises(NotImplementedError, sql.get_params)


class TestAND(unittest.TestCase):

    def test_and(self):
        sql = And([SQLExpression(), SQLExpression()])
        self.assertEqual(normalize_sql(sql.get_sql()), "Null AND Null")
        self.assertEqual(sql.get_params(), {})
        sql = And([SQLExpression(), SQLExpression(), SQLExpression("1 = 1")])
        self.assertEqual(normalize_sql(sql.get_sql()), "Null AND Null AND 1 = 1")
        self.assertEqual(sql.get_params(), {})


class TestOR(unittest.TestCase):

    def test_or(self):
        sql = Or([SQLExpression(), SQLExpression()])
        self.assertEqual(normalize_sql(sql.get_sql()), "Null OR Null")
        self.assertEqual(sql.get_params(), {})


@patch("database.sqlkeymanager.SQLKeyManager", MockKeyManager)
class TestValue(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def test_value(self):
        sql = Value("name", "value", self.SQLKeyManager, "key")
        self.SQLKeyManager.register_key.assert_called_once_with("key")
        self.assertEqual(sql._key, "key")
        self.assertEqual(sql._value, "value")
        self.assertEqual(sql._name, "name")
        self.assertEqual(sql.get_sql(), ":key")
        self.assertEqual(sql.get_params(), {"key": "value"})
        sql = Value("name", "value", self.SQLKeyManager, "key")

    def test_value_with_key_manager_modifications(self):
        manager = MockKeyManagerWithModifications()
        sql = Value("name", "value", manager, "key")
        manager.register_key.assert_called_once_with("key")
        self.assertEqual(sql._key, "key1")
        self.assertEqual(sql.get_sql(), ":key1")
        self.assertEqual(sql.get_params(), {"key1": "value"})


class TestRow(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()
        self.valueList = [
            Value("name", "value", self.SQLKeyManager, "key"),
            Value("name2", "value2", self.SQLKeyManager, "key2"),
        ]

    def test_row(self):
        sql = Row(self.valueList)
        self.assertEqual(sql._values, self.valueList)
        self.assertEqual(sql.get_sql(), "(:key, :key2)")
        self.assertEqual(sql.get_params(), {"key": "value", "key2": "value2"})
        self.assertEqual(sql.names(), "(name, name2)")


class TestSQLColumnDefinition(unittest.TestCase):
    pass
