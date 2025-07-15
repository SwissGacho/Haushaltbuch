import unittest
import re
from unittest.mock import Mock, AsyncMock, create_autospec
from unittest.mock import patch

from database.sql_clause import (
    JoinOperator,
    SQLColumnDefinition,
    From,
    Where,
    GroupBy,
    Having,
    Values,
    Assignment,
)
from database.sql import SQL
from database.sql_expression import ColumnName, Row, SQLExpression, Value, Eq
from business_objects.bo_descriptors import BOColumnFlag


class SQLStatementMockFactory:
    @staticmethod
    def create(prod):
        mock = create_autospec(SQL, instance=True, spec_set=True)
        sql_factory = Mock()
        sql_factory.get_sql_class.return_value = prod
        mock._get_db = Mock(return_value=Mock(sql_factory=sql_factory))
        return mock


def normalize_sql(sql):
    return re.sub("  +", " ", sql).strip()


class SQLColumnDefinitionMock(SQLColumnDefinition):
    type_map = {int: "MOCKINTEGER", str: "MOCKSTRING"}
    constraint_map = {
        BOColumnFlag.BOC_NOT_NULL: "MOCKNOTNULL",
        BOColumnFlag.BOC_UNIQUE: "MOCKUNIQUE",
        BOColumnFlag.BOC_DEFAULT: "MOCKDEFAULT ({default})",
    }


class Test_100_SQLColumnDefinition(unittest.TestCase):

    def setUp(self):
        self.mock_parent = SQLStatementMockFactory.create(SQLColumnDefinitionMock)

    def test_101_init(self):
        self.assertRaises(
            ValueError,
            SQLColumnDefinition,
            "mock_column",
            int,
            parent=SQLStatementMockFactory.create(SQLColumnDefinition),
        )

    def test_102_init_derived(self):
        sql = SQLColumnDefinitionMock("mock_column", int, parent=self.mock_parent)
        self.assertEqual(normalize_sql(sql.get_query()), "mock_column MOCKINTEGER")
        self.assertEqual(sql.params, {})

    def test_103_init_derived_with_constraint(self):
        sql = SQLColumnDefinitionMock(
            "mock_column", int, BOColumnFlag.BOC_NOT_NULL, parent=self.mock_parent
        )
        self.assertEqual(
            normalize_sql(sql.get_query()), "mock_column MOCKINTEGER MOCKNOTNULL"
        )
        self.assertEqual(sql.params, {})

    def test_104_init_derived_with_multiple_constraints(self):
        sql = SQLColumnDefinitionMock(
            "mock_column",
            int,
            BOColumnFlag.BOC_UNIQUE | BOColumnFlag.BOC_NOT_NULL,
            parent=self.mock_parent,
        )
        self.assertEqual(
            normalize_sql(sql.get_query()),
            "mock_column MOCKINTEGER MOCKNOTNULL MOCKUNIQUE",
        )
        self.assertEqual(sql.params, {})

    def test_105_init_derived_with_multiple_constraints_and_arg(self):
        sql = SQLColumnDefinitionMock(
            "mock_column",
            int,
            BOColumnFlag.BOC_DEFAULT | BOColumnFlag.BOC_NOT_NULL,
            parent=self.mock_parent,
            default="mock_default",
        )
        self.assertEqual(
            normalize_sql(sql.get_query()),
            "mock_column MOCKINTEGER MOCKNOTNULL MOCKDEFAULT (mock_default)",
        )
        self.assertEqual(sql.params, {})


class Test_200_From(unittest.TestCase):
    def setUp(self):
        self.mock_parent = SQLStatementMockFactory.create(From)

    def test_201_init(self):
        sql = From("mock_table", parent=self.mock_parent)
        self.assertEqual(sql.get_query(), " FROM mock_table ")

    def test_202_join(self):
        sql = From("mock_table", parent=self.mock_parent)
        join = sql.join("mock_table_2", None)
        self.assertIs(sql, join)
        self.assertEqual(
            normalize_sql(sql.get_query()),
            "FROM mock_table FULL OUTER JOIN mock_table_2",
        )

    def test_203_join_with_condition(self):
        sql = From("mock_table", parent=self.mock_parent).join(
            "mock_table_2", Eq(ColumnName("mock_table.id"), "mock_table_2.id")
        )
        self.assertEqual(
            normalize_sql(sql.get_query()),
            "FROM mock_table FULL OUTER JOIN mock_table_2 ON (mock_table.id = mock_table_2.id)",
        )

    def test_204_join_with_left_join(self):
        sql = From("mock_table", parent=self.mock_parent).join(
            "mock_table_2", Eq("mock_table.id", "mock_table_2.id"), JoinOperator.LEFT
        )
        self.assertEqual(
            sql.get_query().replace("  ", " ").strip(),
            "FROM mock_table LEFT JOIN mock_table_2 ON (mock_table.id = mock_table_2.id)",
        )

    def test_205_join_with_multiple_joins(self):
        sql = (
            From("mock_table", parent=self.mock_parent)
            .join(
                "mock_table_2",
                Eq("mock_table.id", "mock_table_2.id"),
                JoinOperator.LEFT,
            )
            .join("mock_table_3", None)
        )
        self.assertEqual(
            sql.get_query().replace("  ", " ").strip(),
            "FROM mock_table LEFT JOIN mock_table_2 ON (mock_table.id = mock_table_2.id) FULL OUTER JOIN mock_table_3",
        )


class Test_300_Where(unittest.TestCase):
    def setUp(self):
        self.mock_parent = SQLStatementMockFactory.create(Where)

    def test_301_query(self):
        sql = Where(
            Eq(ColumnName("mock_condition"), Value("mick")), parent=self.mock_parent
        )
        self.assertEqual(
            normalize_sql(sql.get_query()), "WHERE (mock_condition = :param)"
        )
        self.assertEqual(sql.params, {"param": "mick"})

    def test_302_mock_get_query_call(self):
        with patch(
            "database.sql_expression.SQLExpression.get_query",
            return_value="mock_condition",
        ) as mock_get_query:
            sql = Where(SQLExpression("mock_condition"), parent=self.mock_parent)
            result = sql.get_query()
        self.assertEqual(normalize_sql(result), "WHERE mock_condition")
        mock_get_query.assert_called_once_with(km=sql)


class Test_400_GroupBy(unittest.TestCase):
    def setUp(self):
        self.mock_parent = SQLStatementMockFactory.create(GroupBy)

    def test_401_init(self):
        sql = GroupBy(["column1", "column2"], parent=self.mock_parent)
        self.assertEqual(normalize_sql(sql.get_query()), "GROUP BY column1, column2")

    def test_402_empty_group_by(self):
        self.assertRaises(ValueError, lambda: GroupBy([], parent=self.mock_parent))

    def test_403_group_by_single_column(self):
        sql = GroupBy("column1", parent=self.mock_parent)
        self.assertEqual(normalize_sql(sql.get_query()), "GROUP BY column1")


class Test_500_Having(unittest.TestCase):
    def setUp(self):
        self.mock_parent = SQLStatementMockFactory.create(Having)

    def test_501_init(self):
        condition = Eq(ColumnName("column1"), Value("value1"))
        sql = Having(condition, parent=self.mock_parent)
        self.assertEqual(normalize_sql(sql.get_query()), "HAVING (column1 = :param)")
        self.assertEqual(sql.params, {"param": "value1"})

    def test_502_mock_get_query_call(self):
        with patch(
            "database.sql_expression.SQLExpression.get_query",
            return_value="mock_condition",
        ) as mock_get_query:
            sql = Having(SQLExpression("mock_condition"), parent=self.mock_parent)
            result = sql.get_query()
        self.assertEqual(normalize_sql(result), "HAVING mock_condition")
        mock_get_query.assert_called_once_with(km=sql)


class Test_600_Assignment(unittest.TestCase):
    def setUp(self):
        self.mock_parent = SQLStatementMockFactory.create(Assignment)

    def test_601_assignment_single_ColumnName(self):
        sql = Assignment(
            ColumnName("column1"), Value("value1"), parent=self.mock_parent
        )
        self.assertEqual(normalize_sql(sql.get_query()), "(column1) = :param")
        self.assertEqual(sql.params, {"param": "value1"})

    def test_602_assignment_single_str_column(self):
        sql = Assignment("column1", Value("value1"), parent=self.mock_parent)
        self.assertEqual(normalize_sql(sql.get_query()), "(column1) = :param")
        self.assertEqual(sql.params, {"param": "value1"})

    def test_603_assignment_multiple_columns(self):
        sql = Assignment(
            [ColumnName("column1"), "column2"],
            Value("value1"),
            parent=self.mock_parent,
        )
        self.assertEqual(normalize_sql(sql.get_query()), "(column1,column2) = :param")
        self.assertEqual(sql.params, {"param": "value1"})

    def test_604_mock_get_query_calls(self):
        with patch(
            "database.sql_expression.Value.get_query", return_value="mock_val"
        ) as mock_value_get_query, patch(
            "database.sql_expression.ColumnName.get_query", return_value="mock_col"
        ) as mock_col_get_query:
            sql = Assignment(
                ColumnName("column1"), Value("value1"), parent=self.mock_parent
            )
            result = sql.get_query()
        self.assertEqual(normalize_sql(result), "(mock_col) = mock_val")
        mock_value_get_query.assert_called_once_with(km=sql)
        mock_col_get_query.assert_called_once_with(km=sql)


class Test_700_Values(unittest.TestCase):
    def setUp(self):
        self.mock_parent = SQLStatementMockFactory.create(Values)
        self.row_list = [
            Row(
                [Value("mick", "value"), Value("mack", "value2")],
            ),
            Row(
                [Value("micky", "value3"), Value("macky", "value4")],
            ),
        ]

    def test_701_values(self):
        sql = Values(self.row_list, parent=self.mock_parent)
        self.assertEqual(sql.get_query(), "VALUES (:mick, :mack), (:micky, :macky)")
        self.assertEqual(
            sql.params,
            {"mick": "value", "mack": "value2", "micky": "value3", "macky": "value4"},
        )
        self.assertEqual(sql.get_names(), "(mick, mack)")

    def test_702_values_row(self):
        sql = (
            Values(parent=self.mock_parent).row(self.row_list[0]).row(self.row_list[1])
        )
        self.assertEqual(sql.get_query(), "VALUES (:mick, :mack), (:micky, :macky)")
        self.assertEqual(
            sql.params,
            {"mick": "value", "mack": "value2", "micky": "value3", "macky": "value4"},
        )
        self.assertEqual(sql.get_names(), "(mick, mack)")
