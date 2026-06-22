import unittest
import re
from unittest.mock import Mock
from unittest.mock import patch

from database.sql_expression import (
    ColumnName,
    Row,
    SQLExpression,
    Not,
    SQLMultiExpression,
    SQLUnaryExpression,
    And,
    Or,
    Eq,
    Is,
    IsNull,
    In,
    SQLString,
    Filter,
    SQLBetween,
    Value,
)


class MockKeyManager:

    def __init__(self):
        self.merge_params.reset_mock()

    merge_params = Mock(return_value="mockmanaged")


def normalize_sql(sql):
    return re.sub("  +", " ", sql).strip()


@patch("database.sql_key_manager.SQLKeyManager", MockKeyManager)
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

    def test_104_column_name(self):
        sql = ColumnName("my_column")
        self.assertEqual(sql.get_query(self.SQLKeyManager), "my_column")
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_105_sql_string(self):
        sql = SQLString("hello")
        self.assertEqual(sql.get_query(self.SQLKeyManager), "'hello'")
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_106_sql_string_empty(self):
        sql = SQLString("")
        self.assertEqual(sql.get_query(self.SQLKeyManager), "''")


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
        self.assertEqual(self.SQLKeyManager.merge_params.call_count, 2)
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 1}
        )
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 77}
        )

    def test_305_not_direct(self):
        sql = Not(SQLExpression("x"))
        self.assertEqual(normalize_sql(sql.get_query(self.SQLKeyManager)), "(NOT x)")
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_306_is_null_direct(self):
        sql = IsNull(ColumnName("col"))
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "(col IS NULL)"
        )
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_307_eq_column_and_string(self):
        sql = Eq(ColumnName("a"), SQLString("b"))
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "(a = 'b')"
        )
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_308_is_direct(self):
        sql = Is(ColumnName("a"), ColumnName("b"))
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "(a IS b)"
        )
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_309_and_empty_raises(self):
        sql = And([])
        self.assertRaises(ValueError, sql.get_query, self.SQLKeyManager)

    def test_310_or_empty_raises(self):
        sql = Or([])
        self.assertRaises(ValueError, sql.get_query, self.SQLKeyManager)

    def test_311_unary_no_operator_raises(self):
        """A SQLUnaryExpression subclass with neither operator set must raise NotImplementedError."""

        class NoOpUnary(SQLUnaryExpression):
            pass

        self.assertRaises(NotImplementedError, NoOpUnary, SQLExpression("x"))

    def test_312_unary_both_operators_raises(self):
        """A SQLUnaryExpression subclass with both operators set must raise ValueError."""

        class BothOpsUnary(SQLUnaryExpression):
            left_operator = "LEFT"
            right_operator = "RIGHT"

        self.assertRaises(ValueError, BothOpsUnary, SQLExpression("x"))


class Test_350_In(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def test_351_string_column_with_sql_strings(self):
        sql = In("col", [SQLString("a"), SQLString("b")])
        self.assertEqual(sql.get_query(self.SQLKeyManager), "col IN ('a', 'b')")
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_352_column_name_object(self):
        sql = In(ColumnName("col"), [SQLString("x")])
        self.assertEqual(sql.get_query(self.SQLKeyManager), "col IN ('x')")
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_353_empty_values_list(self):
        sql = In("col", [])
        self.assertEqual(sql.get_query(self.SQLKeyManager), "col IN ()")

    def test_354_expression_values(self):
        sql = In("col", [SQLExpression(1), SQLExpression(2)])
        self.assertEqual(
            sql.get_query(self.SQLKeyManager), "col IN (mockmanaged, mockmanaged)"
        )
        self.assertEqual(self.SQLKeyManager.merge_params.call_count, 2)
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 1}
        )
        self.SQLKeyManager.merge_params.assert_any_call(
            query=":param", params={"param": 2}
        )


class Test_400_Filter(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = MockKeyManager()

    def test_401_string_eq(self):
        sql = Filter({"name": "alice"})
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "((name = 'alice'))"
        )
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_402_none_value_produces_is_null(self):
        sql = Filter({"age": None})
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "((age IS NULL))"
        )
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_403_int_value(self):
        sql = Filter({"age": 25})
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "((age = mockmanaged))"
        )
        self.SQLKeyManager.merge_params.assert_called_once_with(
            query=":param", params={"param": 25}
        )

    def test_404_multiple_conditions(self):
        sql = Filter({"name": "alice", "deleted": None})
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)),
            "((name = 'alice') AND (deleted IS NULL))",
        )
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_405_sql_expression_key_and_value(self):
        """Keys and values that are already SQLExpression instances are used as-is."""
        sql = Filter({ColumnName("col"): SQLString("val")})
        self.assertEqual(
            normalize_sql(sql.get_query(self.SQLKeyManager)), "((col = 'val'))"
        )
        self.SQLKeyManager.merge_params.assert_not_called()

    def test_406_mixed_none_and_str(self):
        sql = Filter({"a": "x", "b": None, "c": "y"})
        result = normalize_sql(sql.get_query(self.SQLKeyManager))
        self.assertIn("(a = 'x')", result)
        self.assertIn("(b IS NULL)", result)
        self.assertIn("(c = 'y')", result)


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


@patch("database.sql_key_manager.SQLKeyManager", MockKeyManager)
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

    def _test_600_non_string(self, n, v, *args, **kwargs):
        sql = Value(*args, **kwargs)
        self.assertEqual(sql.get_query(km=self.SQLKeyManager), "mockmanaged")
        self.SQLKeyManager.merge_params.assert_called_once_with(
            query=f":{n}", params={f"{n}": v}
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

    def test_608_none_value(self):
        self._test_600_non_string("mick", None, "mick", None)

    def test_609_integer_value(self):
        self._test_600_non_string("mick", 100, "mick", 100)

    def test_610_zero_value(self):
        self._test_600_non_string("mick", 0, "mick", 0)

    def test_611_various_none_cases(self):
        # Value(value=None) → name defaults to "param"
        v = Value(value=None)
        self.assertEqual(v.get_name(), "param")
        self.assertEqual(v.get_query(km=self.SQLKeyManager), "mockmanaged")
        self.SQLKeyManager.merge_params.assert_called_with(
            query=":param", params={"param": None}
        )

        # Value("x", None) → explicit name "x"
        v = Value("x", None)
        self.assertEqual(v.get_name(), "x")
        self.assertEqual(v.get_query(km=self.SQLKeyManager), "mockmanaged")
        self.SQLKeyManager.merge_params.assert_called_with(
            query=":x", params={"x": None}
        )

        # Value(None) → single positional arg treated as value, name defaults to "param"
        v = Value(None)
        self.assertEqual(v.get_name(), "param")
        self.assertEqual(v.get_query(km=self.SQLKeyManager), "mockmanaged")
        self.SQLKeyManager.merge_params.assert_called_with(
            query=":param", params={"param": None}
        )

    def test_612_too_many_positional_args(self):
        self.assertRaises(ValueError, Value, "a", "b", "c")


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

    def test_703_empty_row(self):
        sql = Row()
        self.assertEqual(sql.get_query(km=self.SQLKeyManager), "()")
        self.assertEqual(sql.get_names(), "()")
        self.SQLKeyManager.merge_params.assert_not_called()
