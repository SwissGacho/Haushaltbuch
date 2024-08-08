"""Classes for building SQL expressions that can be used in SQLStatements."""

from enum import Enum, StrEnum, Flag
from typing import TypeAlias
import json

from core.app_logging import getLogger

LOG = getLogger(__name__)


class JoinOperator(Enum):
    """Enum for SQL join operators."""

    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class SQLExpression:
    """Base class for an SQL expression.
    Can be instantiated directly to create an expression verbatim from a string."""

    def __init__(self, expression: str):
        self._expression = "NULL" if expression is None else expression

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return self._expression


class From(SQLExpression):
    """Class for the FROM clause of an SQL statement."""

    def __init__(self, table):
        super().__init__(None)
        self.table = table
        self.joins: list[(JoinOperator, str, SQLExpression)] = []

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        sql = f" FROM {self.table}"
        if len(self.joins) > 0:
            sql += " ".join(
                [f"{join[0]} {join[1]} ON {join[2].sql()}" for join in self.joins]
            )
        return sql

    def join(
        self,
        table=None,
        join_constraint: "SQLExpression" = None,
        join_operator: JoinOperator = JoinOperator.FULL,
    ):
        """Add a join to another table to the FROM clause."""
        self.joins.append((join_operator, table, join_constraint))


class SQLMultiExpression(SQLExpression):
    """Abstract class to combine any number of SQL expressions with an operator.
    Should not be instantiated directly."""

    def __init__(self, arguments: list[SQLExpression]):
        super().__init__(None)
        self.arguments = arguments

    operator: str = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        if self.__class__.operator is None:
            raise NotImplementedError(
                "SQL_multi_expression is an abstract class and should not be instantiated."
            )
        return self.__class__.operator.join(
            [expression.sql() for expression in self.arguments]
        )


class And(SQLMultiExpression):
    """Represents a SQL AND expression."""

    operator = " AND "


class Or(SQLMultiExpression):
    """Represents a SQL OR expression."""

    operator = " OR "


class SQLBinaryExpression(SQLExpression):
    """Abstract class to combine exactly two SQL expressions with an operator.
    Should not be instantiated directly."""

    def __init__(self, left: SQLExpression | str, right: SQLExpression | str):
        super().__init__(None)
        # LOG.debug(f"SQLBinaryExpression({left=}, {right=})")
        self.left = left if isinstance(left, SQLExpression) else SQLExpression(left)
        self.right = right if isinstance(right, SQLExpression) else SQLExpression(right)

    operator = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        if self.__class__.operator is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return f" ({self.left.sql()} {self.__class__.operator} {self.right.sql()}) "


class Eq(SQLBinaryExpression):
    """Represents a SQL = expression."""

    operator = " = "


class Is(SQLBinaryExpression):
    """Represents a SQL 'is' expression."""

    operator = " is "


class IsNull(Is):
    "Represents test for NULL"

    def __init__(self, left: SQLExpression | str):
        super().__init__(left, right=None)


class ColumnName(SQLExpression):
    """Represents a column name"""

    def __init__(self, name: str):
        super().__init__(None)
        self._name = name

    def sql(self) -> str:
        return self._name


class SQLString(SQLExpression):
    """Represents a string value"""

    def __init__(self, value: str):
        super().__init__(None)
        self._value = value

    def sql(self) -> str:
        return f"'{self._value}'"


FilterItem: TypeAlias = SQLExpression | str


class Filter(And):
    """Represent a filter condition matching all items.
    Keys and values of 'filter' are rendered in quotes if they have the class str.
    SQLExpressions are rendered according to their class.
    (Use ColumnName() to avoid rendering in quotes)
    """

    def __init__(self, filters: dict[FilterItem, FilterItem]):
        super().__init__(
            [
                (
                    Eq(
                        var if isinstance(var, SQLExpression) else SQLString(var),
                        val if isinstance(val, SQLExpression) else SQLString(val),
                    )
                    if val is not None
                    else IsNull(
                        var if isinstance(var, SQLExpression) else SQLString(var)
                    )
                )
                for var, val in filters.items()
            ]
        )


class SQLTernaryExpression(SQLExpression):
    """Abstract class to combine exactly three SQL expressions with two operators.
    Should not be instantiated directly."""

    def __init__(
        self,
        first: SQLExpression | str,
        second: SQLExpression | str,
        third: SQLExpression | str,
    ):
        super().__init__(None)
        self.first = first if isinstance(first, SQLExpression) else SQLExpression(first)
        self.second = (
            second if isinstance(second, SQLExpression) else SQLExpression(second)
        )
        self.third = third if isinstance(third, SQLExpression) else SQLExpression(third)

    operator_one = None
    operator_two = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        if self.__class__.operator_one is None or self.__class__.operator_two is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return f" ({self.first.sql()} {self.__class__.operator_one} {self.second.sql()} {self.__class__.operator_two} {self.third.sql()}) "


class SQLBetween(SQLTernaryExpression):
    """Represents a SQL BETWEEN expression."""

    operator_one = " BETWEEN "
    operator_two = " AND "


class Value(SQLExpression):
    """Represents a value in an SQL statement."""

    def __init__(self, name: str, value: any):
        # LOG.debug(f"Value({name=}, {value=})")
        super().__init__(None)
        self._name = name
        self._value = value

    def name(self) -> str:
        "Name of the value"
        return self._name

    def sql(self) -> str:
        if isinstance(self._value, (str, StrEnum, Flag)):
            return f"'{self._value}'"
        if isinstance(self._value, dict | list):
            return f"""'{json.dumps(self._value, separators=(",", ":"))}'"""
        return str(self._value)


class Row(SQLExpression):
    """Represents a list of values defining a row in an SQL statement such as an INSERT."""

    def __init__(self, values: list[Value] = None):
        # LOG.debug(f"Row({values=})")
        super().__init__(None)
        self.values = [] if values is None else values

    def value(self, value: Value):
        """Add a value to the end of the row."""
        self.values.append(value)
        return self

    def names(self) -> str:
        "List of value names"
        return f"({', '.join([v.name() for v in self.values])})"

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"({', '.join([v.sql() for v in self.values])})"


class Values(SQLExpression):
    """Represents a list of rows in an SQL statement such as an INSERT."""

    def __init__(self, rows: list[Row]):
        # LOG.debug(f"Values({rows=})")
        super().__init__(None)
        self.rows = rows

    def row(self, value: Row):
        """Add a row to the end of the list."""
        # LOG.debug(f"Values.row({value=})")
        self.rows.append(value)
        return self

    def names(self) -> str:
        "List of value names"
        if not self.rows:
            return ""
        return self.rows[0].names()

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"VALUES {', '.join([row.sql() for row in self.rows])}"


class Assignment(SQLExpression):
    """Represents an assignment in an SQL statement such as an UPDATE."""

    def __init__(
        self,
        columns: list[str] | str,
        value: Value,
    ):
        super().__init__(None)
        self.columns = [columns] if isinstance(columns, str) else columns
        self.value = value
        self.where: Where = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        sql = "(" + ",".join(self.columns) + ")=" + self.value.sql()
        if self.where is not None:
            sql += self.where.sql()
        return sql


class Where(SQLExpression):
    """Represents a WHERE clause in an SQL statement."""

    def __init__(self, condition: SQLExpression):
        super().__init__(None)
        self.condition = condition

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f" WHERE {self.condition.sql()}"


class GroupBy(SQLExpression):
    """Represents a GROUP BY clause in an SQL statement."""

    def __init__(self, column_list: list[str]):
        super().__init__(None)
        self.column_list = column_list

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"GROUP BY {', '.join(self.column_list)}"


class Having(SQLExpression):
    """Represents a HAVING clause in an SQL statement."""

    def __init__(self, condition: SQLExpression):
        super().__init__(None)
        self.condition = condition

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f" HAVING {self.condition.sql()}"


class SQLColumnDefinition(SQLExpression):
    """Represents the definition of a column in an SQL table."""

    type_map = {}
    constraint_map = {}

    def __init__(self, name: str, data_type: type, constraint: str = None, **pars):
        # LOG.debug(f"SQLColumnDefinition({name=}, {data_type=}, {constraint=}, {pars=})")
        super().__init__(None)
        self.name = name
        if data_type in self.type_map:
            self.data_type = self.__class__.type_map[data_type]
            # LOG.debug(f" - data_type={self.data_type}")
        else:
            raise ValueError(
                f"Unsupported data type for a {self.__class__.__name__}: {data_type}"
            )
        if not constraint:
            self.constraint = ""
        elif constraint in self.constraint_map:
            self.constraint = self.__class__.constraint_map[constraint].format(
                **{
                    k: v.table if hasattr(v, "table") else str(v).lower()
                    for k, v in pars.items()
                }
            )
            # LOG.debug(f" - constraint={self.constraint}")
        else:
            raise ValueError(
                f"Unsupported column constraint for a {self.__class__.__name__}: {constraint}"
            )

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"{self.name} {self.data_type} {self.constraint}"
