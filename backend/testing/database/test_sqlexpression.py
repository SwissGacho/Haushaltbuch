from ast import Eq
from os import name
import unittest
import re
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sql_expression import (
    ColumnName,
    Row,
    SQLExpression,
    Not,
    SQLMultiExpression,
    And,
    Or,
    Eq,
    Is,
    IsNull,
    SQLBetween,
    Value,
)


class MockKeyManager:

    def __init__(self):
        self.merge_params.reset_mock()

    merge_params = Mock(return_value="mockmanaged")


def normalize_sql(sql):
    return re.sub("  +", " ", sql).strip()


@patch("database.sqlkeymanager.SQLKeyManager", MockKeyManager)
class Test_100_SQLExpression(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def test_101_SQLExpressionInstantiation(self):
        sql_expression_instance = SQLExpression()
        self.assertIsNotNone(
            sql_expression_instance, "SQLExpression instance should not be None"
        )

    def test_102_simpleexpression(self):
        sql = SQLExpression(expression="SELECT * FROM table")
        self.assertEqual(sql.get_query(self.SQLKeyManager), "SELECT * FROM table")

    def test_103_expression_with_int(self):
        sql = SQLExpression(expression=99)
        query = sql.get_query(self.SQLKeyManager)
        self.assertEqual(query, "mockmanaged")
        self.SQLKeyManager.merge_params.assert_called_once_with(
            query=":param", params={"param": 99}
        )


class Test_200_SQLMultiExpression(unittest.TestCase):

    def setUp(self):
        self.expressions = [SQLExpression(), SQLExpression(), SQLExpression()]
        self.SQLKeyManager = MockKeyManager()

    def test_201_init(self):
        sql = SQLMultiExpression(self.expressions)
        self.assertEqual(sql._arguments, self.expressions)

    def test_202_not_implemented(self):
        sql = SQLMultiExpression(self.expressions)
        self.assertRaises(NotImplementedError, sql.get_query, self.SQLKeyManager)


class Test_300_SQLExpression(unittest.TestCase):
    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def test_301_and(self):
        sql = And([SQLExpression(1), SQLExpression(), "quack"])
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)),
            "(mockmanaged AND Null AND quack)",
        )
        self.SQLKeyManager.merge_params.assert_called_once_with(
            query=":param", params={"param": 1}
        )

    def test_302_or(self):
        sql = Or([SQLExpression(99), SQLExpression()])
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "(mockmanaged OR Null)"
        )
        self.SQLKeyManager.merge_params.assert_called_once_with(
            query=":param", params={"param": 99}
        )

    def test_303_or_and(self):
        sql = Or([SQLExpression(99), SQLExpression(), And([SQLExpression(1), "quack"])])
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)),
            "(mockmanaged OR Null OR (mockmanaged AND quack))",
        )
        self.assertEqual(self.SQLKeyManager.merge_params.call_count, 2)
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 99}
        )
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 1}
        )

    def test_304_complex_expr(self):
        sql = And(
            [
                SQLExpression(1),
                "complex",
                Not(Eq(ColumnName("A"), "b")),
                Or([SQLExpression(77), IsNull(ColumnName("D"))]),
            ]
        )
        query = sql.get_query(self.SQLKeyManager)
        self.assertEqual(
            normalize_sql(query),
            "(mockmanaged AND complex AND (NOT (A = b)) AND (mockmanaged OR (D IS NULL)))",
        )
        print(
            f"{query=} {self.SQLKeyManager.merge_params.call_count=}, {self.SQLKeyManager.merge_params.call_args_list=}"
        )
        self.assertEqual(self.SQLKeyManager.merge_params.call_count, 2)
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 1}
        )
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 77}
        )


class Test_400_Filter(unittest.TestCase):
    pass


class Test_500_TernaryExpression(unittest.TestCase):
    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def test_501_between(self):
        result = SQLBetween("age", 18, 25)
        self.assertEqual(
            result.get_query(km=self.SQLKeyManager),
            "( age BETWEEN mockmanaged AND mockmanaged )",
        )
        self.assertEqual(self.SQLKeyManager.merge_params.call_count, 2)
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 18}
        )
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 25}
        )


@patch("database.sqlkeymanager.SQLKeyManager", MockKeyManager)
class Test_600_Value(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def _test_600(self, n, v, *args, **kwargs):
        sql = Value(*args, **kwargs)
        self.assertEqual(sql.get_query(km=self.SQLKeyManager), "mockmanaged")
        self.SQLKeyManager.merge_params.assert_called_once_with(
            query=f":{n}", params={f"{n}": f"{v}"}
        )
        self.assertEqual(sql.get_name(), n)

    def test_601_2_args(self):
        self._test_600("mick", "mack", "mick", "mack")

    def test_602_single_arg(self):
        self._test_600("param", "mick", "mick")

    def test_603_no_args(self):
        self.assertRaises(ValueError, Value)

    def test_604_named_args(self):
        self._test_600("mick", "mack", name="mick", value="mack")

    def test_605_no_name(self):
        self._test_600("param", "mack", value="mack")

    def test_606_no_value(self):
        self.assertRaises(ValueError, Value, name="mick")

    def test_607_mixed_args(self):
        self._test_600("mick", "mack", "mack", name="mick")


class Test_700_Row(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()
        self.valueList = [Value("mick", "value"), Value("mack", "value2")]

    def test_701_row(self):
        sql = Row(self.valueList)
        self.assertEqual(
            sql.get_query(km=self.SQLKeyManager), "(mockmanaged, mockmanaged)"
        )
        self.assertEqual(self.SQLKeyManager.merge_params.call_count, 2)
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":mick", params={"mick": "value"}
        )
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":mack", params={"mack": "value2"}
        )
        self.assertEqual(sql.get_names(), "(mick, mack)")

    def test_702_row_value(self):
        sql = Row().value(self.valueList[0]).value(self.valueList[1])
        self.assertEqual(sql._values, self.valueList)
        self.assertEqual(
            sql.get_query(km=self.SQLKeyManager), "(mockmanaged, mockmanaged)"
        )
        self.assertEqual(self.SQLKeyManager.merge_params.call_count, 2)
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":mick", params={"mick": "value"}
        )
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":mack", params={"mack": "value2"}
        )
        self.assertEqual(sql.get_names(), "(mick, mack)")
